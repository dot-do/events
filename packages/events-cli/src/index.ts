/**
 * @dotdo/events-cli - CLI for scaffolding and developing events.do projects
 *
 * This module exports utilities for programmatic use of the CLI commands.
 */

export { initProject, type InitOptions } from './commands/init.js'
export { runDev, type DevOptions } from './commands/dev.js'
export { runDeploy, type DeployOptions } from './commands/deploy.js'
export {
  schemaInit,
  schemaValidate,
  schemaGenerate,
  type SchemaOptions
} from './commands/schema.js'
export { runTail, type TailOptions } from './commands/tail.js'
export { runQuery, type QueryOptions, type QueryResult } from './commands/query.js'
export { runStatus, type StatusOptions, type HealthResponse, type ShardStats } from './commands/status.js'
export { runReplay, type ReplayOptions, type DeadLetter, type ReplayResult } from './commands/replay.js'

// Re-export templates for customization
export { TEMPLATES } from './templates/index.js'
