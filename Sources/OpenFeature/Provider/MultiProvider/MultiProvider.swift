import Combine
import Foundation
import Logging

/// A provider that combines multiple providers into a single provider.
public class MultiProvider: FeatureProvider {
    public var hooks: [any Hook] {
        []
    }

    public static let name = "MultiProvider"
    public var metadata: ProviderMetadata

    private let providers: [FeatureProvider]
    private let strategy: Strategy

    /// Initialize a MultiProvider with a list of providers and a strategy.
    /// - Parameters:
    ///   - providers: A list of providers to evaluate.
    ///   - strategy: A strategy to evaluate the providers. Defaults to FirstMatchStrategy.
    public init(
        providers: [FeatureProvider],
        strategy: Strategy = FirstMatchStrategy()
    ) {
        self.providers = providers
        self.strategy = strategy
        metadata = MultiProviderMetadata(providers: providers)
    }

    public func initialize(
        initialContext: EvaluationContext?, onDone: @escaping @Sendable (Result<Void, Error>) -> Void
    ) {
        dispatchAll(
            providers: providers,
            each: { provider, callback in provider.initialize(initialContext: initialContext, onDone: callback) },
            onDone: onDone
        )
    }

    public func shutdown(onDone: @escaping @Sendable () -> Void) {
        dispatchAll(
            providers: providers,
            each: { provider, callback in provider.shutdown(onDone: { callback(.success(())) }) },
            onDone: { _ in onDone() }
        )
    }

    public func onContextSet(
        oldContext: EvaluationContext?, newContext: EvaluationContext,
        onDone: @escaping @Sendable (Result<Void, Error>) -> Void
    ) {
        dispatchAll(
            providers: providers,
            each: { provider, callback in
                provider.onContextSet(oldContext: oldContext, newContext: newContext, onDone: callback)
            },
            onDone: onDone
        )
    }

    public func getBooleanEvaluation(key: String, defaultValue: Bool, context: EvaluationContext?) throws
        -> ProviderEvaluation<Bool>
    {
        return try getBooleanEvaluation(key: key, defaultValue: defaultValue, context: context, logger: nil)
    }

    public func getStringEvaluation(key: String, defaultValue: String, context: EvaluationContext?) throws
        -> ProviderEvaluation<String>
    {
        return try getStringEvaluation(key: key, defaultValue: defaultValue, context: context, logger: nil)
    }

    public func getIntegerEvaluation(key: String, defaultValue: Int64, context: EvaluationContext?) throws
        -> ProviderEvaluation<Int64>
    {
        return try getIntegerEvaluation(key: key, defaultValue: defaultValue, context: context, logger: nil)
    }

    public func getDoubleEvaluation(key: String, defaultValue: Double, context: EvaluationContext?) throws
        -> ProviderEvaluation<Double>
    {
        return try getDoubleEvaluation(key: key, defaultValue: defaultValue, context: context, logger: nil)
    }

    public func getObjectEvaluation(key: String, defaultValue: Value, context: EvaluationContext?) throws
        -> ProviderEvaluation<Value>
    {
        return try getObjectEvaluation(key: key, defaultValue: defaultValue, context: context, logger: nil)
    }

    // Logger-enabled methods - canonical implementations
    public func getBooleanEvaluation(key: String, defaultValue: Bool, context: EvaluationContext?, logger: Logger?)
        throws
        -> ProviderEvaluation<Bool>
    {
        return try strategy.evaluate(
            providers: providers,
            key: key,
            defaultValue: defaultValue,
            evaluationContext: context
        ) { provider in
            { (key: String, defaultValue: Bool, context: EvaluationContext?) throws -> ProviderEvaluation<Bool> in
                try provider.getBooleanEvaluation(
                    key: key, defaultValue: defaultValue, context: context, logger: logger)
            }
        }
    }

    public func getStringEvaluation(key: String, defaultValue: String, context: EvaluationContext?, logger: Logger?)
        throws
        -> ProviderEvaluation<String>
    {
        return try strategy.evaluate(
            providers: providers,
            key: key,
            defaultValue: defaultValue,
            evaluationContext: context
        ) { provider in
            { (key: String, defaultValue: String, context: EvaluationContext?) throws -> ProviderEvaluation<String> in
                try provider.getStringEvaluation(key: key, defaultValue: defaultValue, context: context, logger: logger)
            }
        }
    }

    public func getIntegerEvaluation(key: String, defaultValue: Int64, context: EvaluationContext?, logger: Logger?)
        throws
        -> ProviderEvaluation<Int64>
    {
        return try strategy.evaluate(
            providers: providers,
            key: key,
            defaultValue: defaultValue,
            evaluationContext: context
        ) { provider in
            { (key: String, defaultValue: Int64, context: EvaluationContext?) throws -> ProviderEvaluation<Int64> in
                try provider.getIntegerEvaluation(
                    key: key, defaultValue: defaultValue, context: context, logger: logger)
            }
        }
    }

    public func getDoubleEvaluation(key: String, defaultValue: Double, context: EvaluationContext?, logger: Logger?)
        throws
        -> ProviderEvaluation<Double>
    {
        return try strategy.evaluate(
            providers: providers,
            key: key,
            defaultValue: defaultValue,
            evaluationContext: context
        ) { provider in
            { (key: String, defaultValue: Double, context: EvaluationContext?) throws -> ProviderEvaluation<Double> in
                try provider.getDoubleEvaluation(key: key, defaultValue: defaultValue, context: context, logger: logger)
            }
        }
    }

    public func getObjectEvaluation(key: String, defaultValue: Value, context: EvaluationContext?, logger: Logger?)
        throws
        -> ProviderEvaluation<Value>
    {
        return try strategy.evaluate(
            providers: providers,
            key: key,
            defaultValue: defaultValue,
            evaluationContext: context
        ) { provider in
            { (key: String, defaultValue: Value, context: EvaluationContext?) throws -> ProviderEvaluation<Value> in
                try provider.getObjectEvaluation(key: key, defaultValue: defaultValue, context: context, logger: logger)
            }
        }
    }

    public func observe() -> AnyPublisher<ProviderEvent, Never> {
        return Publishers.MergeMany(providers.map { $0.observe() }).eraseToAnyPublisher()
    }

    public struct MultiProviderMetadata: ProviderMetadata {
        public var name: String?

        init(providers: [FeatureProvider]) {
            name =
                "MultiProvider: "
                + providers.map {
                    $0.metadata.name ?? "Provider"
                }
                .joined(separator: ", ")
        }
    }
}

/// Runs `body(provider, callback)` for each provider; when all callbacks have been invoked,
/// invokes `onDone` with the first error if any, otherwise success.
/// For void-only operations (e.g. shutdown), have the body call `callback(.success(()))` when done.
private func dispatchAll(
    providers: [FeatureProvider],
    each body: (FeatureProvider, @escaping @Sendable (Result<Void, Error>) -> Void) -> Void,
    onDone: @escaping @Sendable (Result<Void, Error>) -> Void
) {
    /// Thread-safe shared state for aggregating results from concurrent callbacks.
    final class SharedState: @unchecked Sendable {
        let lock = NSLock()
        var value: Error?
    }
    let state = SharedState()
    let group = DispatchGroup()
    for provider in providers {
        group.enter()
        body(provider) { result in
            defer { group.leave() }
            if case .failure(let error) = result {
                state.lock.withLock { if state.value == nil { state.value = error } }
            }
        }
    }
    group.notify(queue: .global()) {
        if let error = state.lock.withLock({ state.value }) {
            onDone(.failure(error))
        } else {
            onDone(.success(()))
        }
    }
}
