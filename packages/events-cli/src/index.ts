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

// Re-export templates for customization
export { TEMPLATES } from './templates/index.js'
