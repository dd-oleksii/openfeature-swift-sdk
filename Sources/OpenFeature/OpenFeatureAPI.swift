import Combine
import Foundation
import Logging

/// A global singleton which holds base configuration for the OpenFeature library.
/// Configuration here will be shared across all ``Client``s.
public class OpenFeatureAPI {
    // Sync queue to change state atomically
    private let stateQueue = DispatchQueue(label: "com.openfeature.state.queue")

    private(set) var driverSubject = CurrentValueSubject<ProviderDriver, Never>(ProviderDriver())
    private(set) var evaluationContext: EvaluationContext?
    private(set) var hooks: [any Hook] = []
    private var logger: Logger?

    internal struct OpenFeatureState {
        let driver: ProviderDriver
        let evaluationContext: EvaluationContext?
        let hooks: [any Hook]
        let logger: Logger?
    }

    /// The ``OpenFeatureAPI`` singleton
    static public let shared = OpenFeatureAPI()

    public init() {}

    /**
    Set provider and calls its `initialize` in a background thread.
    Readiness can be determined from `getState` or listening for `ready` event.
    */
    public func setProvider(provider: FeatureProvider, initialContext: EvaluationContext?) {
        _ = setProviderInternal(provider: provider, initialContext: initialContext)
    }

    /**
    Set provider and calls its `initialize`.
    This async function returns when the `initialize` from the provider is completed.
    */
    public func setProviderAndWait(provider: FeatureProvider, initialContext: EvaluationContext?) async {
        _ = try? await setProviderInternal(provider: provider, initialContext: initialContext).value
    }

    /**
    Set provider and calls its `initialize` in a background thread.
    Readiness can be determined from `getState` or listening for `ready` event.
    */
    public func setProvider(provider: FeatureProvider) {
        setProvider(provider: provider, initialContext: nil)
    }

    /**
    Set provider and calls its `initialize`.
    This async function returns when the `initialize` from the provider is completed.
    */
    public func setProviderAndWait(provider: FeatureProvider) async {
        await setProviderAndWait(provider: provider, initialContext: nil)
    }

    public func getProvider() -> FeatureProvider? {
        return stateQueue.sync {
            self.driverSubject.value.wrappedProvider
        }
    }

    public func clearProvider() {
        _ = clearProviderInternal()
    }

    /**
    Clear provider.
    This async function returns when the clear operation is completed.
    */
    public func clearProviderAndWait() async {
        await clearProviderInternal().value
    }

    /// Shuts down the current provider and resets all API state (provider, evaluation context, hooks).
    /// Per spec: API MUST define a function to propagate a shutdown request to all providers,
    /// and MUST reset all state of the API, removing all hooks, event handlers, and providers.
    public func shutdown() {
        _ = shutdownInternal()
    }

    /// Shuts down the current provider and resets all API state. Returns when the provider's shutdown has completed.
    public func shutdownAndWait() async {
        await shutdownInternal().value
    }

    private func shutdownInternal() -> Future<Void, Never> {
        return stateQueue.sync {
            let currentDriver = self.driverSubject.value
            self.driverSubject.send(ProviderDriver())
            self.evaluationContext = nil
            self.hooks.removeAll()
            self.logger = nil
            return currentDriver.shutdown()
        }
    }

    private func clearProviderInternal() -> Future<Void, Never> {
        return stateQueue.sync { () -> Future<Void, Never> in
            let currentDriver = self.driverSubject.value
            self.driverSubject.send(ProviderDriver())
            return currentDriver.shutdown()
        }
    }

    /**
    Set evaluation context and calls the provider's `onContextSet` in a background thread.
    Readiness can be determined from `getState` or listening for `contextChanged` event.
    */
    public func setEvaluationContext(evaluationContext: EvaluationContext) {
        _ = updateContext(evaluationContext: evaluationContext)
    }

    /**
    Set evaluation context and calls the provider's `onContextSet`.
    This async function returns when the `onContextSet` from the provider is completed.
    */
    public func setEvaluationContextAndWait(evaluationContext: EvaluationContext) async {
        _ = try? await updateContext(evaluationContext: evaluationContext).value
    }

    public func getEvaluationContext() -> EvaluationContext? {
        return stateQueue.sync {
            self.evaluationContext
        }
    }

    public func getProviderStatus() -> ProviderStatus {
        return stateQueue.sync {
            self.driverSubject.value.status
        }
    }

    public func getProviderMetadata() -> ProviderMetadata? {
        return stateQueue.sync {
            self.driverSubject.value.metadata
        }
    }

    public func getClient() -> Client {
        return OpenFeatureClient(openFeatureApi: self, name: nil, version: nil)
    }

    public func getClient(name: String?, version: String?) -> Client {
        return OpenFeatureClient(openFeatureApi: self, name: name, version: version)
    }

    public func addHooks(hooks: (any Hook)...) {
        stateQueue.sync {
            self.hooks.append(contentsOf: hooks)
        }
    }

    public func clearHooks() {
        stateQueue.sync {
            self.hooks.removeAll()
        }
    }

    internal func getHooks() -> [any Hook] {
        return stateQueue.sync {
            self.hooks
        }
    }

    public func setLogger(_ logger: Logger?) {
        stateQueue.sync {
            self.logger = logger
        }
    }

    public func getLogger() -> Logger? {
        return stateQueue.sync {
            self.logger
        }
    }

    public func observe() -> AnyPublisher<ProviderEvent, Never> {
        return
            driverSubject
            .map { $0.observe() }
            .switchToLatest()
            .eraseToAnyPublisher()
    }

    internal func getState() -> OpenFeatureState {
        return stateQueue.sync {
            OpenFeatureState(
                driver: driverSubject.value,
                evaluationContext: evaluationContext,
                hooks: hooks,
                logger: logger
            )
        }
    }

    /// Updates state and invokes the driver's initialize on stateQueue; returns the Future for callers that await.
    private func setProviderInternal(provider: FeatureProvider, initialContext: EvaluationContext? = nil)
        -> Future<Void, Error>
    {
        let driver = ProviderDriver(provider: provider)
        return stateQueue.sync { () -> Future<Void, Error> in
            let oldDriver = self.driverSubject.value
            self.driverSubject.send(driver)
            if let initialContext = initialContext {
                self.evaluationContext = initialContext
            }
            // Shutdown old provider in background without blocking new provider initialization
            _ = oldDriver.shutdown()
            return driver.initialize(initialContext: initialContext)
        }
    }

    /// Updates state and invokes the driver's onContextSet on stateQueue; returns the Future for callers that await.
    private func updateContext(evaluationContext: EvaluationContext) -> Future<Void, Error> {
        return stateQueue.sync { () -> Future<Void, Error> in
            let oldContext = self.evaluationContext
            self.evaluationContext = evaluationContext
            return self.driverSubject.value.onContextSet(oldContext: oldContext, newContext: evaluationContext)
        }
    }
}