import { Command } from 'commander'
import pc from 'picocolors'
import { spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { resolve } from 'node:path'

export interface DevOptions {
  port?: number
  local?: boolean
  persist?: boolean
  config?: string
}

export async function runDev(options: DevOptions = {}): Promise<void> {
  const cwd = process.cwd()
  const wranglerConfig = options.config || 'wrangler.jsonc'
  const configPath = resolve(cwd, wranglerConfig)

  // Check if wrangler config exists
  if (!existsSync(configPath) && !existsSync(resolve(cwd, 'wrangler.toml'))) {
    console.log(pc.red('\n  Error: No wrangler.jsonc or wrangler.toml found.'))
    console.log(pc.dim('  Run `events init` to create a new project or ensure you are in the correct directory.\n'))
    return
  }

  console.log(pc.cyan('\n  Starting events.do development server...\n'))

  const args = ['wrangler', 'dev']

  if (options.port) {
    args.push('--port', String(options.port))
  }

  if (options.local) {
    args.push('--local')
  }

  if (options.persist !== false) {
    args.push('--persist')
  }

  if (options.config) {
    args.push('--config', options.config)
  }

  // Try to detect package manager
  const packageManager = detectPackageManager(cwd)

  console.log(pc.dim(`  Using ${packageManager} to run wrangler...\n`))

  const child = spawn(packageManager === 'npm' ? 'npx' : packageManager, args, {
    cwd,
    stdio: 'inherit',
    env: {
      ...process.env,
      FORCE_COLOR: '1',
    },
  })

  child.on('error', (err) => {
    console.error(pc.red(`\n  Failed to start dev server: ${err.message}\n`))
    process.exit(1)
  })

  child.on('exit', (code) => {
    process.exit(code ?? 0)
  })
}

function detectPackageManager(cwd: string): 'npm' | 'pnpm' | 'yarn' {
  if (existsSync(resolve(cwd, 'pnpm-lock.yaml'))) return 'pnpm'
  if (existsSync(resolve(cwd, 'yarn.lock'))) return 'yarn'
  return 'npm'
}

export const devCommand = new Command('dev')
  .description('Start local development server')
  .option('-p, --port <port>', 'Port to listen on', parseInt)
  .option('-l, --local', 'Run in local-only mode (no remote resources)')
  .option('--no-persist', 'Disable persistent storage')
  .option('-c, --config <file>', 'Path to wrangler config file')
  .action(async (opts) => {
    await runDev(opts)
  })
