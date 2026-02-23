import Combine
import Foundation
import Logging

// Relevant spec requirements:
//
// Status management:
//   1.7.3  (MUST):  Status indicates READY if initialize terminates normally.
//   1.7.4  (MUST):  Status indicates ERROR if initialize terminates abnormally.
//   1.7.5  (MUST):  Status indicates FATAL if initialize terminates abnormally with error code PROVIDER_FATAL.
//   1.7.9  (MUST):  Status indicates NOT_READY once shutdown terminates.
//
// Provider events:
//   5.1.1  (MAY):   Provider may signal PROVIDER_READY, PROVIDER_ERROR, PROVIDER_CONFIGURATION_CHANGED,
//                    PROVIDER_STALE. Providers cannot emit PROVIDER_CONTEXT_CHANGED or PROVIDER_RECONCILING.
//
// Events and initialization:
//   5.3.1  (MUST):  If initialize terminates normally, PROVIDER_READY handlers must run.
//   5.3.2  (MUST):  If initialize terminates abnormally, PROVIDER_ERROR handlers must run.
//   5.3.3  (MUST):  Handlers attached after the provider is already in the associated state must run immediately.
//
// Events and context reconciliation (static-context paradigm):
//   5.3.4.1 (MUST): While on context changed is executing, RECONCILING handlers must run.
//   5.3.4.2 (MUST): If on context changed terminates normally, and no other invocations have yet to terminate,
//                    PROVIDER_CONTEXT_CHANGED handlers must run.
//   5.3.4.3 (MUST): If on context changed terminates abnormally, and no other invocations have yet to terminate,
//                    PROVIDER_ERROR handlers must run.
//
// Event-to-status coherence:
//   5.3.5  (MUST):  Status must be updated before the SDK invokes any event handlers for that event.
//
// Short-circuit (do not call provider resolver):
//   1.7.6  (MUST):  Default, run error hooks, and indicate error if resolution attempted while NOT_READY.
//   1.7.7  (MUST):  Default, run error hooks, and indicate error if resolution attempted while FATAL.
//   The SDK avoids calling the provider's resolver functions entirely when the provider is in these states.
//
// Error propagation:
//   1.7.8  (SHOULD): Implementations should propagate the error code from provider lifecycle methods.
//
// Event-to-status mapping (5.3.5 table):
//   PROVIDER_READY                 -> READY
//   PROVIDER_STALE                 -> STALE
//   PROVIDER_ERROR                 -> ERROR (or FATAL if error code is PROVIDER_FATAL)
//   PROVIDER_CONFIGURATION_CHANGED -> no status change
//   PROVIDER_CONTEXT_CHANGED       -> READY (emitted by SDK only, not by provider)
//   PROVIDER_RECONCILING           -> RECONCILING (emitted by SDK only, not by provider)

/// InternalProviderEvent extends the ProviderEvent with events that are emitted by the ProviderDriver itself.
///
/// Internal events are processed serially by `StatusTracker` to make sure that:
/// 1. The order of events is always consistent with the order of status changes.
/// 2. Any promise associated with an action (e.g., initialize, contextSet, shutdown) is resolved after the action is complete and the status has been updated accordingly.
/// 3. Status changes happen before any of subscribers receive the corresponding event.
private enum InternalProviderEvent {
    /// Event emitted by the provider.
    case providerEvent(ProviderEvent)
    case initializeStarted
    case initializeDone(Result<Void, Error>, promise: Future<Void, Error>.Promise)
    case contextSetStarted
    case contextSetDone(Result<Void, Error>, promise: Future<Void, Error>.Promise)
    case shutdownStarted
    case shutdownDone(promise: Future<Void, Never>.Promise)

    /// Resolves the promise associated with the event.
    func resolve() {
        switch self {
        case .providerEvent: break
        case .initializeStarted: break
        case .initializeDone(let result, let promise):
            promise(result)
        case .contextSetStarted: break
        case .contextSetDone(let result, let promise):
            promise(result)
        case .shutdownStarted: break
        case .shutdownDone(let promise):
            promise(.success(()))
        }
    }
}

/// Default metadata when no provider is set.
private struct NoProviderMetadata: ProviderMetadata {
    var name: String? { "No provider" }
}

/// ProviderDriver is a wrapper around a FeatureProvider that manages the lifecycle, eventing, and status for the provider.
///
/// When `provider` is nil, lifecycle methods no-op (return immediately-completed Futures without updating status),
/// so status stays NOT_READY. Evaluation and track short-circuit via ensureNotShortCircuited().
///
/// After the provider is wrapped into the driver, the only way to interact with it is through the driver interface.
///
/// Important: there must be at most one driver per provider. Otherwise, the provider may receive conflicting lifecycle calls and status may go whacky.
///
/// # Threading Model
///
/// All public methods of `ProviderDriver` are thread-safe and can be called from any thread:
/// - `initialize(initialContext:)` - Can be called concurrently; multiple calls will be serialized
/// - `shutdown()` - Can be called concurrently; multiple calls will be serialized
/// - `onContextSet(oldContext:newContext:)` - Can be called concurrently; overlapping calls are tracked and serialized
/// - `evaluate(key:defaultValue:context:logger:)` - Thread-safe; can be called from any thread including during lifecycle transitions
/// - `track(key:context:details:)` - Thread-safe; can be called from any thread
/// - `observe()` - Returns a thread-safe publisher; subscribers receive events on arbitrary threads
///
/// The driver uses `ProviderStatusTracker` to ensure thread-safety:
/// - All status updates and event emissions are serialized via `ProviderStatusTracker.serializationLock`
/// - Status reads use a separate `statusLock` for efficient concurrent access from evaluation paths
/// - Provider lifecycle methods (initialize, shutdown, onContextSet) execute on arbitrary threads but their completion
///   is serialized through the status tracker to ensure coherent status transitions
/// - Event subscribers always observe status changes before receiving the corresponding event (spec 5.3.5)
///
/// Concurrent lifecycle calls are safe:
/// - Multiple concurrent `initialize()` calls are serialized; only the first has effect
/// - Multiple concurrent `onContextSet()` calls are tracked; only the last-completing call emits final events
/// - Concurrent `evaluate()` calls proceed independently and check status via lock-protected reads
internal final class ProviderDriver: @unchecked Sendable {
    private let provider: FeatureProvider?

    private let statusTracker = ProviderStatusTracker()
    private var cancellables = Set<AnyCancellable>()

    /// Exposed for API/client use. Pass-through to the wrapped provider, or empty when no provider.
    var hooks: [any Hook] { provider?.hooks ?? [] }

    /// Exposed for API/client use. Pass-through to the wrapped provider, or default when no provider.
    var metadata: ProviderMetadata { provider?.metadata ?? NoProviderMetadata() }

    /// Current provider status from the status tracker.
    var status: ProviderStatus { statusTracker.status }

    /// The wrapped provider, or nil when no provider is set. Exposed for API (e.g. getProvider()).
    var wrappedProvider: FeatureProvider? { provider }

    /// No-provider driver; status remains NOT_READY.
    internal init() {
        self.provider = nil
    }

    internal init(provider: FeatureProvider) {
        self.provider = provider

        // Subscribe to provider-generated events.
        provider.observe().sink { @Sendable [weak self] event in
            self?.statusTracker.process(.providerEvent(event))
        }.store(in: &cancellables)
    }

    func observe() -> AnyPublisher<ProviderEvent, Never> {
        return statusTracker.eraseToAnyPublisher()
    }

    func initialize(initialContext: EvaluationContext?) -> Future<Void, Error> {
        guard let provider = provider else {
            return Future { $0(.success(())) }
        }
        return Future { promise in
            self.statusTracker.process(.initializeStarted)
            provider.initialize(initialContext: initialContext) { @Sendable result in
                self.statusTracker.process(.initializeDone(result, promise: promise))
            }
        }
    }

    func shutdown() -> Future<Void, Never> {
        guard let provider = provider else {
            return Future { $0(.success(())) }
        }
        return Future { promise in
            self.statusTracker.process(.shutdownStarted)
            provider.shutdown { @Sendable in
                self.statusTracker.process(.shutdownDone(promise: promise))
            }
        }
    }

    func onContextSet(oldContext: EvaluationContext?, newContext: EvaluationContext) -> Future<Void, Error> {
        guard let provider = provider else {
            return Future { $0(.success(())) }
        }
        return Future { promise in
            self.statusTracker.process(.contextSetStarted)
            provider.onContextSet(oldContext: oldContext, newContext: newContext) { result in
                self.statusTracker.process(.contextSetDone(result, promise: promise))
            }
        }
    }

    /// Throws without calling the provider when status is NOT_READY or FATAL (spec 1.7.6, 1.7.7).
    private func ensureNotShortCircuited() throws {
        switch statusTracker.status {
        case .notReady:
            throw OpenFeatureError.providerNotReadyError
        case .fatal:
            throw OpenFeatureError.providerFatalError(message: "Provider is in fatal state")
        case .ready, .stale, .reconciling, .error:
            break
        }
    }

    /// Generic evaluation that dispatches to the appropriate typed provider method.
    /// Throws for NOT_READY/FATAL (short-circuit) or when the provider throws.
    func evaluate<T: AllowedFlagValueType>(
        key: String,
        defaultValue: T,
        context: EvaluationContext?,
        logger: Logger?
    ) throws -> ProviderEvaluation<T> {
        guard let provider = provider else {
            throw OpenFeatureError.providerNotReadyError
        }
        try ensureNotShortCircuited()
        switch T.flagValueType {
        case .boolean:
            guard let defaultValue = defaultValue as? Bool else {
                throw OpenFeatureError.generalError(message: "Unable to match default value type with flag value type")
            }
            return try provider.getBooleanEvaluation(
                key: key, defaultValue: defaultValue, context: context, logger: logger)
                as! ProviderEvaluation<T>
        case .string:
            guard let defaultValue = defaultValue as? String else {
                throw OpenFeatureError.generalError(message: "Unable to match default value type with flag value type")
            }
            return try provider.getStringEvaluation(
                key: key, defaultValue: defaultValue, context: context, logger: logger)
                as! ProviderEvaluation<T>
        case .integer:
            guard let defaultValue = defaultValue as? Int64 else {
                throw OpenFeatureError.generalError(message: "Unable to match default value type with flag value type")
            }
            return try provider.getIntegerEvaluation(
                key: key, defaultValue: defaultValue, context: context, logger: logger)
                as! ProviderEvaluation<T>
        case .double:
            guard let defaultValue = defaultValue as? Double else {
                throw OpenFeatureError.generalError(message: "Unable to match default value type with flag value type")
            }
            return try provider.getDoubleEvaluation(
                key: key, defaultValue: defaultValue, context: context, logger: logger)
                as! ProviderEvaluation<T>
        case .object:
            guard let defaultValue = defaultValue as? Value else {
                throw OpenFeatureError.generalError(message: "Unable to match default value type with flag value type")
            }
            return try provider.getObjectEvaluation(
                key: key, defaultValue: defaultValue, context: context, logger: logger)
                as! ProviderEvaluation<T>
        }
    }

    func track(key: String, context: (any EvaluationContext)?, details: (any TrackingEventDetails)?) throws {
        try self.provider!.track(key: key, context: context, details: details)
    }
}

/// ProviderStatusTracker processes internal events to update provider status, and publishes
/// events for downstream subscribers. It serializes all processing via serializationLock,
/// to make sure that all downstream subscribers observer events and status changes in a coherent order.
///
/// ProviderStatusTracker implements a custom `Publisher`/`Subscription` instead of exposing
/// `downstreamEvents` directly because each subscriber needs:
/// 1. replay an initial event derived from the current status, and
/// 2. atomically transition from that replay snapshot to live event streaming.
///
/// The custom subscription performs replay and sink installation under
/// `serializationLock`, preventing races where a subscriber could miss an event
/// between reading current status and attaching to the live stream.
private final class ProviderStatusTracker: Publisher, @unchecked Sendable {
    typealias Output = ProviderEvent
    typealias Failure = Never

    // Serializes lifecycle/provider event handling so transitions, promise resolution, and
    // downstream emissions happen in a coherent order.
    private let serializationLock = NSRecursiveLock()
    // True from initializeStarted until initializeDone. Used to ignore contextSetDone that finish before init.
    private var initializationInProgress = false
    // Number of outstanding onContextSet calls. This is used to make sure that we only respect the last-returning onContextSet call.
    private var contextUpdatesInProgress = 0
    // Publishes events for downstream subscribers (after replay).
    private let downstreamEvents = PassthroughSubject<ProviderEvent, Never>()

    // Protects the status field for narrow-scope reads/writes.
    private let statusLock = NSLock()
    private var _status: ProviderStatus = .notReady
    private(set) var status: ProviderStatus {
        // Keep status reads/writes lock-protected because evaluation paths may read frequently
        // from threads that do not hold serializationLock.
        get { statusLock.withLock { _status } }
        set { statusLock.withLock { _status = newValue } }
    }

    func process(_ internalEvent: InternalProviderEvent) {
        serializationLock.withLock {
            // We need to make sure that our own handler runs first, so that the downstream subscribers see a coherent status.
            let events = apply(internalEvent: internalEvent)

            for event in events {
                downstreamEvents.send(event)
            }

            // Promise resolution and downstream emission happen after state transition.
            internalEvent.resolve()
        }
    }

    private func apply(internalEvent: InternalProviderEvent) -> [ProviderEvent] {
        func handleError(_ error: Error) -> [ProviderEvent] {
            switch error {
            case OpenFeatureError.providerFatalError(let message):
                status = .fatal
                return [.error(ProviderEventDetails(message: message, errorCode: .providerFatal))]
            default:
                status = .error
                return [.error(ProviderEventDetails(message: error.localizedDescription))]
            }
        }

        switch internalEvent {
        case .initializeStarted:
            initializationInProgress = true
            return []
        case .initializeDone(.success, _):
            initializationInProgress = false
            // Handle the edge case where onContextSet is called while initialize is still running.
            // This can occur when the context is changed before the provider finishes initialization.
            // In this scenario:
            // 1. initialize() starts and transitions to RECONCILING
            // 2. onContextSet() is called (possibly multiple times) while initialize is in progress
            // 3. initialize() completes successfully
            // 4. We check if any context updates are still in progress (contextUpdatesInProgress > 0)
            // 5. If context updates are still running, we transition to RECONCILING instead of READY
            // 6. This ensures we stay in RECONCILING state until all context updates complete
            // 7. Only after the final context update finishes do we transition to READY
            if contextUpdatesInProgress > 0 {
                // onContextSet is still in progress; stay in reconciling until it finishes.
                status = .reconciling
                return [.reconciling(nil)]
            }
            status = .ready
            return [.ready(nil)]
        case .initializeDone(.failure(let error), _):
            initializationInProgress = false
            return handleError(error)
        case .contextSetStarted:
            status = .reconciling
            contextUpdatesInProgress += 1
            if contextUpdatesInProgress > 1 {
                // Collapse overlapping context updates into a single reconciling window.
                return []
            }
            return [.reconciling(nil)]
        case .contextSetDone(let result, _):
            contextUpdatesInProgress -= 1
            if initializationInProgress || contextUpdatesInProgress > 0 {
                // Only the last finishing context update is allowed to emit final events.
                return []
            }
            switch result {
            case .success:
                status = .ready
                // We need to emit .ready in addition to .contextChanged, so that subscribers
                // that only watch for .ready will see it.
                return [.ready(nil), .contextChanged(nil)]
            case .failure(let error):
                return handleError(error)
            }
        case .shutdownStarted:
            status = .notReady
            return []
        case .shutdownDone:
            // No event to emit.
            return []
        case .providerEvent(let event):
            switch event {
            case .ready:
                status = .ready
                return [event]
            case .error(ProviderEventDetails(errorCode: .providerFatal)):
                status = .fatal
                return [event]
            case .error:
                status = .error
                return [event]
            case .configurationChanged:
                // No status change.
                return [event]
            case .stale:
                status = .stale
                return [event]
            case .reconciling, .contextChanged:
                // Not allowed to be emitted by the provider.
                return []
            }
        }
    }

    func receive<S>(subscriber: S) where S: Subscriber, Never == S.Failure, ProviderEvent == S.Input {
        let subscription = TrackerSubscription(subscriber: subscriber, tracker: self)
        subscriber.receive(subscription: subscription)
    }

    private final class TrackerSubscription<S: Subscriber>: Subscription
    where S.Input == ProviderEvent, S.Failure == Never {
        private var subscriber: S?
        private weak var tracker: ProviderStatusTracker?
        private var demand: Subscribers.Demand = .none
        private var started = false
        private var downstreamCancellable: AnyCancellable?
        private var isCancelled = false

        init(subscriber: S, tracker: ProviderStatusTracker) {
            self.subscriber = subscriber
            self.tracker = tracker
        }

        func request(_ newDemand: Subscribers.Demand) {
            guard newDemand > .none else { return }
            guard let tracker else { return }

            // Replay snapshot and subscribe under the same lock to avoid missing events
            // between the snapshot read and sink installation.
            tracker.serializationLock.withLock {
                guard !isCancelled else { return }
                demand += newDemand

                if !started {
                    started = true

                    if let event = TrackerSubscription.replayEvent(for: tracker.status) {
                        emit(event)
                    }

                    downstreamCancellable = tracker.downstreamEvents.sink { [weak self] event in
                        self?.emit(event)
                    }
                }
            }
        }

        func cancel() {
            guard let tracker else {
                isCancelled = true
                downstreamCancellable?.cancel()
                downstreamCancellable = nil
                subscriber = nil
                return
            }

            tracker.serializationLock.withLock {
                isCancelled = true
                downstreamCancellable?.cancel()
                downstreamCancellable = nil
                subscriber = nil
            }
        }

        private func emit(_ event: ProviderEvent) {
            guard let subscriber else { return }
            guard demand > .none else { return }

            if demand != .unlimited {
                demand -= 1
            }
            demand += subscriber.receive(event)
        }

        private static func replayEvent(for status: ProviderStatus) -> ProviderEvent? {
            switch status {
            case .notReady:
                return nil
            case .ready:
                return .ready(nil)
            case .error:
                return .error(nil)
            case .stale:
                return .stale(nil)
            case .fatal:
                return .error(ProviderEventDetails(errorCode: .providerFatal))
            case .reconciling:
                return .reconciling(nil)
            }
        }
    }
}
