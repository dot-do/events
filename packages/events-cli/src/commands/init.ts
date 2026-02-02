import { Command } from 'commander'
import pc from 'picocolors'
import prompts from 'prompts'
import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { TEMPLATES } from '../templates/index.js'

export interface InitOptions {
  name?: string
  template?: 'basic' | 'cdc' | 'analytics'
  typescript?: boolean
  packageManager?: 'npm' | 'pnpm' | 'yarn'
  skipInstall?: boolean
}

export async function initProject(options: InitOptions = {}): Promise<void> {
  console.log(pc.cyan('\n  events.do Project Setup\n'))

  // Interactive prompts for missing options
  const answers = await prompts([
    {
      type: options.name ? null : 'text',
      name: 'name',
      message: 'Project name:',
      initial: 'my-events-project',
      validate: (value: string) =>
        /^[a-z0-9-]+$/.test(value) || 'Project name must be lowercase alphanumeric with hyphens',
    },
    {
      type: options.template ? null : 'select',
      name: 'template',
      message: 'Select a template:',
      choices: [
        { title: 'Basic - Simple event streaming', value: 'basic' },
        { title: 'CDC - Change Data Capture with collections', value: 'cdc' },
        { title: 'Analytics - Full lakehouse with DuckDB queries', value: 'analytics' },
      ],
    },
    {
      type: options.packageManager ? null : 'select',
      name: 'packageManager',
      message: 'Package manager:',
      choices: [
        { title: 'pnpm (recommended)', value: 'pnpm' },
        { title: 'npm', value: 'npm' },
        { title: 'yarn', value: 'yarn' },
      ],
    },
  ])

  const config = {
    name: options.name || answers.name,
    template: options.template || answers.template,
    packageManager: options.packageManager || answers.packageManager,
    typescript: options.typescript !== false,
  }

  if (!config.name || !config.template) {
    console.log(pc.yellow('\n  Setup cancelled.\n'))
    return
  }

  const projectDir = resolve(process.cwd(), config.name)

  // Check if directory exists
  if (existsSync(projectDir)) {
    console.log(pc.red(`\n  Error: Directory "${config.name}" already exists.\n`))
    return
  }

  console.log(pc.dim(`\n  Creating project in ${projectDir}...\n`))

  // Create project structure
  mkdirSync(projectDir, { recursive: true })
  mkdirSync(join(projectDir, 'src'), { recursive: true })
  mkdirSync(join(projectDir, 'schemas'), { recursive: true })

  // Get template content - ensure we get a project template (not common)
  const templateName = config.template as 'basic' | 'cdc' | 'analytics'
  const template = TEMPLATES[templateName]

  // Write files
  writeFileSync(
    join(projectDir, 'package.json'),
    JSON.stringify(template.packageJson(config.name), null, 2)
  )

  writeFileSync(
    join(projectDir, 'wrangler.jsonc'),
    template.wranglerConfig(config.name)
  )

  writeFileSync(
    join(projectDir, 'tsconfig.json'),
    TEMPLATES.common.tsconfig()
  )

  writeFileSync(
    join(projectDir, 'src/index.ts'),
    template.indexTs()
  )

  writeFileSync(
    join(projectDir, '.gitignore'),
    TEMPLATES.common.gitignore()
  )

  writeFileSync(
    join(projectDir, 'schemas/events.json'),
    TEMPLATES.common.eventSchema()
  )

  console.log(pc.green('  Project created successfully!\n'))

  // Next steps
  console.log(pc.cyan('  Next steps:\n'))
  console.log(pc.dim(`    cd ${config.name}`))
  console.log(pc.dim(`    ${config.packageManager} install`))
  console.log(pc.dim(`    ${config.packageManager === 'npm' ? 'npx' : config.packageManager} events dev`))
  console.log()
}

export const initCommand = new Command('init')
  .description('Scaffold a new events.do project')
  .option('-n, --name <name>', 'Project name')
  .option('-t, --template <template>', 'Template to use (basic, cdc, analytics)')
  .option('--no-typescript', 'Use JavaScript instead of TypeScript')
  .option('-p, --package-manager <pm>', 'Package manager (npm, pnpm, yarn)')
  .option('--skip-install', 'Skip installing dependencies')
  .action(async (opts) => {
    await initProject(opts)
  })
