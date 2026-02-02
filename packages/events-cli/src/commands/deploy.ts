import { Command } from 'commander'
import pc from 'picocolors'
import { spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { resolve } from 'node:path'

export interface DeployOptions {
  env?: string
  dryRun?: boolean
  config?: string
  minify?: boolean
}

export async function runDeploy(options: DeployOptions = {}): Promise<void> {
  const cwd = process.cwd()
  const wranglerConfig = options.config || 'wrangler.jsonc'
  const configPath = resolve(cwd, wranglerConfig)

  // Check if wrangler config exists
  if (!existsSync(configPath) && !existsSync(resolve(cwd, 'wrangler.toml'))) {
    console.log(pc.red('\n  Error: No wrangler.jsonc or wrangler.toml found.'))
    console.log(pc.dim('  Run `events init` to create a new project or ensure you are in the correct directory.\n'))
    return
  }

  console.log(pc.cyan('\n  Deploying to Cloudflare Workers...\n'))

  if (options.dryRun) {
    console.log(pc.yellow('  [DRY RUN] Would deploy with the following options:'))
    console.log(pc.dim(`    Environment: ${options.env || 'production'}`))
    console.log(pc.dim(`    Config: ${wranglerConfig}`))
    console.log(pc.dim(`    Minify: ${options.minify !== false ? 'yes' : 'no'}\n`))
    return
  }

  const args = ['wrangler', 'deploy']

  if (options.env) {
    args.push('--env', options.env)
  }

  if (options.config) {
    args.push('--config', options.config)
  }

  if (options.minify !== false) {
    args.push('--minify')
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
    console.error(pc.red(`\n  Failed to deploy: ${err.message}\n`))
    process.exit(1)
  })

  child.on('exit', (code) => {
    if (code === 0) {
      console.log(pc.green('\n  Deployment successful!\n'))
    }
    process.exit(code ?? 0)
  })
}

function detectPackageManager(cwd: string): 'npm' | 'pnpm' | 'yarn' {
  if (existsSync(resolve(cwd, 'pnpm-lock.yaml'))) return 'pnpm'
  if (existsSync(resolve(cwd, 'yarn.lock'))) return 'yarn'
  return 'npm'
}

export const deployCommand = new Command('deploy')
  .description('Deploy to Cloudflare Workers')
  .option('-e, --env <environment>', 'Deploy to specific environment')
  .option('--dry-run', 'Show what would be deployed without deploying')
  .option('-c, --config <file>', 'Path to wrangler config file')
  .option('--no-minify', 'Disable minification')
  .action(async (opts) => {
    await runDeploy(opts)
  })
