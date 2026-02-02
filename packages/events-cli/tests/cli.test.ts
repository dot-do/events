import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { existsSync, rmSync, readFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

// Mock prompts to avoid interactive input
vi.mock('prompts', () => ({
  default: vi.fn().mockResolvedValue({
    name: 'test-project',
    template: 'basic',
    packageManager: 'pnpm',
  }),
}))

describe('CLI Commands', () => {
  const testDir = join(tmpdir(), `events-cli-test-${Date.now()}`)

  beforeEach(() => {
    mkdirSync(testDir, { recursive: true })
    process.chdir(testDir)
  })

  afterEach(() => {
    rmSync(testDir, { recursive: true, force: true })
  })

  describe('init command', () => {
    it('should create project structure with basic template', async () => {
      const { initProject } = await import('../src/commands/init.js')

      await initProject({
        name: 'my-test-project',
        template: 'basic',
        packageManager: 'pnpm',
      })

      const projectDir = join(testDir, 'my-test-project')

      expect(existsSync(projectDir)).toBe(true)
      expect(existsSync(join(projectDir, 'package.json'))).toBe(true)
      expect(existsSync(join(projectDir, 'wrangler.jsonc'))).toBe(true)
      expect(existsSync(join(projectDir, 'tsconfig.json'))).toBe(true)
      expect(existsSync(join(projectDir, 'src/index.ts'))).toBe(true)
      expect(existsSync(join(projectDir, '.gitignore'))).toBe(true)
      expect(existsSync(join(projectDir, 'schemas/events.json'))).toBe(true)

      // Verify package.json content
      const pkg = JSON.parse(readFileSync(join(projectDir, 'package.json'), 'utf-8'))
      expect(pkg.name).toBe('my-test-project')
      expect(pkg.dependencies['@dotdo/events']).toBeDefined()
    })

    it('should create project with CDC template', async () => {
      const { initProject } = await import('../src/commands/init.js')

      await initProject({
        name: 'cdc-project',
        template: 'cdc',
        packageManager: 'npm',
      })

      const projectDir = join(testDir, 'cdc-project')
      const indexTs = readFileSync(join(projectDir, 'src/index.ts'), 'utf-8')

      expect(indexTs).toContain('CDCCollection')
      expect(indexTs).toContain('ItemsDO')
    })

    it('should create project with analytics template', async () => {
      const { initProject } = await import('../src/commands/init.js')

      await initProject({
        name: 'analytics-project',
        template: 'analytics',
        packageManager: 'yarn',
      })

      const projectDir = join(testDir, 'analytics-project')
      const indexTs = readFileSync(join(projectDir, 'src/index.ts'), 'utf-8')
      const wrangler = readFileSync(join(projectDir, 'wrangler.jsonc'), 'utf-8')

      expect(indexTs).toContain('CatalogDO')
      expect(indexTs).toContain('buildQuery')
      expect(wrangler).toContain('LAKEHOUSE_BUCKET')
      expect(wrangler).toContain('analytics_engine_datasets')
    })

    it('should not overwrite existing directory', async () => {
      const { initProject } = await import('../src/commands/init.js')

      // Create directory first
      mkdirSync(join(testDir, 'existing-project'))

      const consoleSpy = vi.spyOn(console, 'log')

      await initProject({
        name: 'existing-project',
        template: 'basic',
        packageManager: 'pnpm',
      })

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('already exists')
      )
    })
  })

  describe('schema commands', () => {
    it('should initialize schemas directory', async () => {
      const { schemaInit } = await import('../src/commands/schema.js')

      schemaInit({ dir: 'schemas' })

      expect(existsSync(join(testDir, 'schemas'))).toBe(true)
      expect(existsSync(join(testDir, 'schemas/events.json'))).toBe(true)
      expect(existsSync(join(testDir, 'schemas/custom-event.json'))).toBe(true)
    })

    it('should validate valid schemas', async () => {
      const { schemaInit, schemaValidate } = await import('../src/commands/schema.js')

      schemaInit({ dir: 'schemas' })
      const isValid = schemaValidate({ dir: 'schemas' })

      expect(isValid).toBe(true)
    })

    it('should detect invalid schemas', async () => {
      const { schemaValidate } = await import('../src/commands/schema.js')

      const schemasDir = join(testDir, 'schemas')
      mkdirSync(schemasDir, { recursive: true })

      // Write invalid JSON
      writeFileSync(join(schemasDir, 'invalid.json'), 'not valid json')

      const isValid = schemaValidate({ dir: 'schemas' })

      expect(isValid).toBe(false)
    })

    it('should generate TypeScript types from schemas', async () => {
      const { schemaInit, schemaGenerate } = await import('../src/commands/schema.js')

      schemaInit({ dir: 'schemas' })
      schemaGenerate({ dir: 'schemas' })

      const typesFile = join(testDir, 'src/schema-types.ts')
      expect(existsSync(typesFile)).toBe(true)

      const content = readFileSync(typesFile, 'utf-8')
      expect(content).toContain('export interface Events')
      expect(content).toContain('export interface CustomEvent')
    })
  })

  describe('dev command', () => {
    it('should error when no wrangler config exists', async () => {
      const { runDev } = await import('../src/commands/dev.js')
      const consoleSpy = vi.spyOn(console, 'log')

      await runDev({})

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('No wrangler.jsonc or wrangler.toml found')
      )
    })
  })

  describe('deploy command', () => {
    it('should error when no wrangler config exists', async () => {
      const { runDeploy } = await import('../src/commands/deploy.js')
      const consoleSpy = vi.spyOn(console, 'log')

      await runDeploy({})

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('No wrangler.jsonc or wrangler.toml found')
      )
    })

    it('should support dry run mode', async () => {
      const { runDeploy } = await import('../src/commands/deploy.js')

      // Create a minimal wrangler config
      writeFileSync(
        join(testDir, 'wrangler.jsonc'),
        JSON.stringify({ name: 'test', main: 'src/index.ts' })
      )

      const consoleSpy = vi.spyOn(console, 'log')

      await runDeploy({ dryRun: true })

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('[DRY RUN]')
      )
    })
  })
})

describe('Templates', () => {
  it('should export all templates', async () => {
    const { TEMPLATES } = await import('../src/templates/index.js')

    expect(TEMPLATES.common).toBeDefined()
    expect(TEMPLATES.basic).toBeDefined()
    expect(TEMPLATES.cdc).toBeDefined()
    expect(TEMPLATES.analytics).toBeDefined()
  })

  it('should generate valid package.json for each template', async () => {
    const { TEMPLATES } = await import('../src/templates/index.js')

    for (const templateName of ['basic', 'cdc', 'analytics'] as const) {
      const template = TEMPLATES[templateName]
      const pkg = template.packageJson('test-project')

      expect(pkg.name).toBe('test-project')
      expect(pkg.dependencies).toBeDefined()
      expect(pkg.devDependencies).toBeDefined()
      expect(pkg.scripts?.dev).toBeDefined()
      expect(pkg.scripts?.deploy).toBeDefined()
    }
  })

  it('should generate valid wrangler config for each template', async () => {
    const { TEMPLATES } = await import('../src/templates/index.js')

    for (const templateName of ['basic', 'cdc', 'analytics'] as const) {
      const template = TEMPLATES[templateName]
      const config = template.wranglerConfig('test-project')

      expect(config).toContain('"name": "test-project"')
      expect(config).toContain('"main": "src/index.ts"')
      expect(config).toContain('EVENTS_BUCKET')
    }
  })
})
