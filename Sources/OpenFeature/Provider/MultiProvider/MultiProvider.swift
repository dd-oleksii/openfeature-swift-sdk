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

    public func initialize(initialContext: EvaluationContext?) -> Future<Void, Never> {
        return Future { promise in
            dispatchAll(
                providers: self.providers,
                each: { provider, callback in
                    var cancellable: AnyCancellable?
                    cancellable = provider.initialize(initialContext: initialContext).sink { _ in
                        withExtendedLifetime(cancellable) {}
                        callback()
                    }
                },
                onDone: { promise(.success(())) }
            )
        }
    }

    public func onContextSet(
        oldContext: EvaluationContext?,
        newContext: EvaluationContext
    ) -> Future<Void, Never> {
        return Future { promise in
            dispatchAll(
                providers: self.providers,
                each: { provider, callback in
                    var cancellable: AnyCancellable?
                    cancellable = provider.onContextSet(oldContext: oldContext, newContext: newContext).sink { _ in
                        withExtendedLifetime(cancellable) {}
                        callback()
                    }
                },
                onDone: { promise(.success(())) }
            )
        }
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
/// invokes `onDone`.
private func dispatchAll(
    providers: [FeatureProvider],
    each body: (FeatureProvider, @escaping @Sendable () -> Void) -> Void,
    onDone: @escaping @Sendable () -> Void
) {
    let group = DispatchGroup()
    for provider in providers {
        group.enter()
        body(provider) {
            group.leave()
        }
    }
    group.notify(queue: .global()) {
        onDone()
    }
}
