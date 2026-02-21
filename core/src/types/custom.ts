import type { BaseEvent } from './base.js'

export interface CustomEvent extends BaseEvent<`custom.${string}`, Record<string, unknown>> {}
