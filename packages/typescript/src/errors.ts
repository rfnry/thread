export class ThreadException extends Error {
  readonly code: string = 'internal'

  constructor(message: string) {
    super(message)
    this.name = new.target.name
  }
}

export class NotFoundError extends ThreadException {
  override readonly code: string = 'not_found'
}

export class InputError extends ThreadException {
  override readonly code: string = 'input'
}

export class ConfigurationError extends ThreadException {
  override readonly code: string = 'configuration'
}

export class ForbiddenError extends ThreadException {
  override readonly code: string = 'forbidden'
}

export class StorageError extends ThreadException {
  override readonly code: string = 'storage'
}

export class TransportError extends ThreadException {
  override readonly code: string = 'transport'
}

const CLASSES: Record<string, new (message: string) => ThreadException> = {
  internal: ThreadException,
  not_found: NotFoundError,
  input: InputError,
  configuration: ConfigurationError,
  forbidden: ForbiddenError,
  storage: StorageError,
  transport: TransportError,
}

export function raised(code: string, message: string): ThreadException {
  return new (CLASSES[code] ?? ThreadException)(message)
}
