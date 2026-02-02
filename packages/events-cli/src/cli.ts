import { Command } from 'commander'
import pc from 'picocolors'
import { initCommand } from './commands/init.js'
import { devCommand } from './commands/dev.js'
import { deployCommand } from './commands/deploy.js'
import { schemaCommand } from './commands/schema.js'
import { tailCommand } from './commands/tail.js'
import { queryCommand } from './commands/query.js'
import { statusCommand } from './commands/status.js'
import { replayCommand } from './commands/replay.js'

const VERSION = '0.1.0'

const program = new Command()

program
  .name('events')
  .description('CLI for scaffolding and developing events.do projects')
  .version(VERSION)

// Add subcommands
program.addCommand(initCommand)
program.addCommand(devCommand)
program.addCommand(deployCommand)
program.addCommand(schemaCommand)
program.addCommand(tailCommand)
program.addCommand(queryCommand)
program.addCommand(statusCommand)
program.addCommand(replayCommand)

// Display help if no command is provided
program.action(() => {
  console.log(pc.cyan(`
  events.do CLI v${VERSION}

  Event streaming, CDC, and lakehouse analytics for Durable Objects.
  `))
  program.help()
})

// Error handling
program.exitOverride()

try {
  await program.parseAsync(process.argv)
} catch (err) {
  if (err instanceof Error && err.message !== '(outputHelp)') {
    console.error(pc.red(`Error: ${err.message}`))
    process.exit(1)
  }
}
