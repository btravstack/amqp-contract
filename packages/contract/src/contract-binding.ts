import { defineContract } from "./builder/contract.js";
import { deriveTtlBackoffInfrastructure } from "./builder/ttl-backoff.js";
import type {
  BindingDefinition,
  ContractDefinition,
  ExchangeDefinition,
  PublisherDefinition,
  QueueDefinition,
} from "./types.js";

/** Explicit physical names for resources present in a normalized contract. */
export type ContractBinding = {
  exchanges?: Readonly<Record<string, string>>;
  queues?: Readonly<Record<string, string>>;
};

type Name<N, M> = N extends keyof M ? M[N] : N;
type Exchanges<B extends ContractBinding> = "exchanges" extends keyof B
  ? NonNullable<B["exchanges"]>
  : Record<never, never>;
type Queues<B extends ContractBinding> = "queues" extends keyof B
  ? NonNullable<B["queues"]>
  : Record<never, never>;
type Exchange<T, B extends ContractBinding> = {
  [K in keyof T]: K extends "name"
    ? Name<T[K], Exchanges<B>>
    : K extends "arguments"
      ? Record<string, unknown>
      : T[K];
};
type Queue<T, B extends ContractBinding> = {
  [K in keyof T]: K extends "name"
    ? Name<T[K], Queues<B>>
    : K extends "deadLetter"
      ? DeadLetter<T[K], B>
      : K extends "arguments"
        ? Record<string, unknown>
        : T[K];
};
type DeadLetter<T, B extends ContractBinding> = {
  [K in keyof T]: K extends "exchange"
    ? Exchange<T[K], B>
    : K extends "routingKey"
      ? T extends { exchange: { name: "" } }
        ? Name<T[K], Queues<B>>
        : T[K]
      : T[K];
};
type Entry<T, B extends ContractBinding> = {
  [K in keyof T]: K extends "exchange" | "source" | "destination"
    ? Exchange<T[K], B>
    : K extends "queue"
      ? Queue<T[K], B>
      : K extends "routingKey"
        ? T extends { exchange: { name: "" } }
          ? Name<T[K], Queues<B>>
          : T[K]
        : T[K];
};

/** Protocol keys and schemas survive binding; topology maps use physical names. */
export type BoundContract<C extends ContractDefinition, B extends ContractBinding> = BoundProtocol<
  Pick<C, keyof C & keyof ContractDefinition>,
  B
>;

type BoundProtocol<C, B extends ContractBinding> = {
  [S in keyof C]: S extends "exchanges"
    ? { [K in keyof C[S] as Name<K, Exchanges<B>> & PropertyKey]: Exchange<C[S][K], B> }
    : S extends "queues"
      ? { [K in keyof C[S] as Name<K, Queues<B>> & PropertyKey]: Queue<C[S][K], B> }
      : S extends "publishers" | "consumers" | "bindings" | "rpcs"
        ? { [K in keyof C[S]]: Entry<C[S][K], B> }
        : C[S];
};

/**
 * Materialize explicitly mapped deployment resources without changing message
 * schemas or logical operation keys. Unmapped shared producer resources stay put.
 * Queue names also determine RPC destinations and derived TTL-backoff resources.
 */
export function defineContractBinding<
  C extends ContractDefinition,
  const B extends ContractBinding,
>(contract: C, binding: B): BoundContract<C, B>;
export function defineContractBinding(
  contract: ContractDefinition,
  binding: ContractBinding,
): object {
  validateMapping(contract.exchanges ?? {}, binding.exchanges ?? {});
  validateMapping(contract.queues ?? {}, binding.queues ?? {});
  const exchangeName = (name: string) =>
    (binding.exchanges && Object.hasOwn(binding.exchanges, name)
      ? binding.exchanges[name]
      : undefined) ?? name;
  const queueName = (name: string) =>
    (binding.queues && Object.hasOwn(binding.queues, name) ? binding.queues[name] : undefined) ??
    name;
  const args = (
    value: Record<string, unknown>,
    typedDeadLetterExchange: string | undefined,
  ): Record<string, unknown> => {
    const result = { ...value };
    for (const key of ["alternate-exchange", "x-dead-letter-exchange"]) {
      const name = result[key];
      if (typeof name === "string") result[key] = exchangeName(name);
    }
    if (
      (typedDeadLetterExchange ?? value["x-dead-letter-exchange"]) === "" &&
      typeof value["x-dead-letter-routing-key"] === "string"
    ) {
      result["x-dead-letter-routing-key"] = queueName(value["x-dead-letter-routing-key"]);
    }
    return result;
  };
  const exchange = <T extends ExchangeDefinition>(
    value: T,
  ): Omit<T, "name"> & { name: string } => ({
    ...value,
    name: exchangeName(value.name),
    ...(value.arguments ? { arguments: args(value.arguments, undefined) } : {}),
  });
  const queue = (value: QueueDefinition): QueueDefinition => ({
    ...value,
    name: queueName(value.name),
    ...(value.arguments
      ? { arguments: args(value.arguments, value.deadLetter?.exchange.name) }
      : {}),
    ...(value.deadLetter
      ? {
          deadLetter: {
            ...value.deadLetter,
            exchange: exchange(value.deadLetter.exchange),
            ...(value.deadLetter.exchange.name === "" && value.deadLetter.routingKey !== undefined
              ? { routingKey: queueName(value.deadLetter.routingKey) }
              : {}),
          },
        }
      : {}),
  });
  const publisher = (value: PublisherDefinition): PublisherDefinition => {
    if (value.routingKey !== undefined) {
      return {
        ...value,
        exchange: exchange(value.exchange),
        routingKey: value.exchange.name === "" ? queueName(value.routingKey) : value.routingKey,
      };
    }
    return { ...value, exchange: exchange(value.exchange) };
  };
  const bind = (value: BindingDefinition): BindingDefinition => {
    if (value.type === "queue") {
      return Object.assign({}, value, {
        queue: queue(value.queue),
        exchange: exchange(value.exchange),
      });
    }
    return Object.assign({}, value, {
      source: exchange(value.source),
      destination: exchange(value.destination),
    });
  };
  const queues = map(contract.queues, queue);
  const occupied = new Set(Object.values(queues).map((value) => value.name));
  for (const value of Object.values(queues)) {
    for (const wait of deriveTtlBackoffInfrastructure(value)?.waitQueues ?? []) {
      validateName(wait.name);
      if (occupied.has(wait.name)) fail(`derived retry queue collision: ${wait.name}`);
      occupied.add(wait.name);
    }
  }
  return defineContract({
    exchanges: map(contract.exchanges, exchange),
    queues,
    bindings: map(contract.bindings, bind),
    publishers: map(contract.publishers, publisher),
    consumers: map(contract.consumers, (value) => ({ ...value, queue: queue(value.queue) })),
    rpcs: map(contract.rpcs, (value) => ({ ...value, queue: queue(value.queue) })),
  });
}

function map<T, U>(
  values: Record<string, T> | undefined,
  transform: (value: T) => U,
): Record<string, U> {
  return Object.fromEntries(
    Object.entries(values ?? {}).map(([key, value]) => [key, transform(value)]),
  );
}

function validateMapping(
  resources: Record<string, unknown>,
  mapping: Readonly<Record<string, string>>,
): void {
  for (const [name, physical] of Object.entries(mapping)) {
    if (!Object.hasOwn(resources, name)) fail(`unknown resource: ${name}`);
    validateName(name);
    validateName(physical);
  }
  const names = Object.keys(resources).map((name) => mapping[name] ?? name);
  if (new Set(names).size !== names.length) fail("physical resource names collide");
}

function validateName(name: string): void {
  if (
    typeof name !== "string" ||
    name.trim() !== name ||
    name.length === 0 ||
    name.startsWith("amq.") ||
    new TextEncoder().encode(name).length > 255
  ) {
    fail(`invalid or reserved resource name: ${name}`);
  }
}

function fail(message: string): never {
  // oxlint-disable-next-line unthrown/no-throw -- fail-fast declaration-time binding configuration error
  throw new Error(`defineContractBinding: ${message}`);
}
