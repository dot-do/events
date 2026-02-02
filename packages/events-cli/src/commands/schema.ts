import { Command } from 'commander'
import pc from 'picocolors'
import { existsSync, mkdirSync, readFileSync, writeFileSync, readdirSync } from 'node:fs'
import { join, resolve, basename } from 'node:path'
import { z } from 'zod'

export interface SchemaOptions {
  dir?: string
}

// JSON Schema validator using Zod for basic structure validation
const JsonSchemaValidator = z.object({
  $schema: z.string().optional(),
  $id: z.string().optional(),
  title: z.string().optional(),
  description: z.string().optional(),
  type: z.enum(['object', 'array', 'string', 'number', 'boolean', 'null']).optional(),
  properties: z.record(z.any()).optional(),
  required: z.array(z.string()).optional(),
  additionalProperties: z.boolean().optional(),
})

export function schemaInit(options: SchemaOptions = {}): void {
  const schemasDir = resolve(process.cwd(), options.dir || 'schemas')

  if (!existsSync(schemasDir)) {
    mkdirSync(schemasDir, { recursive: true })
    console.log(pc.green(`\n  Created schemas directory: ${schemasDir}\n`))
  }

  // Create example schema files
  const exampleSchemas = {
    'events.json': {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      $id: 'events.json',
      title: 'Event Schema',
      description: 'Base schema for events.do events',
      type: 'object',
      properties: {
        type: {
          type: 'string',
          description: 'Event type identifier',
        },
        ts: {
          type: 'string',
          format: 'date-time',
          description: 'ISO 8601 timestamp',
        },
        do: {
          type: 'object',
          properties: {
            id: { type: 'string' },
            name: { type: 'string' },
            class: { type: 'string' },
          },
          required: ['id'],
        },
      },
      required: ['type', 'ts', 'do'],
    },
    'custom-event.json': {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      $id: 'custom-event.json',
      title: 'Custom Event',
      description: 'Example custom event schema',
      type: 'object',
      allOf: [{ $ref: 'events.json' }],
      properties: {
        type: {
          const: 'custom.example',
        },
        data: {
          type: 'object',
          properties: {
            userId: { type: 'string' },
            action: { type: 'string' },
            metadata: { type: 'object' },
          },
          required: ['userId', 'action'],
        },
      },
      required: ['data'],
    },
  }

  for (const [filename, schema] of Object.entries(exampleSchemas)) {
    const filepath = join(schemasDir, filename)
    if (!existsSync(filepath)) {
      writeFileSync(filepath, JSON.stringify(schema, null, 2))
      console.log(pc.dim(`  Created ${filename}`))
    } else {
      console.log(pc.yellow(`  Skipped ${filename} (already exists)`))
    }
  }

  console.log(pc.green('\n  Schema initialization complete!\n'))
}

export function schemaValidate(options: SchemaOptions = {}): boolean {
  const schemasDir = resolve(process.cwd(), options.dir || 'schemas')

  if (!existsSync(schemasDir)) {
    console.log(pc.red(`\n  Error: Schemas directory not found: ${schemasDir}`))
    console.log(pc.dim('  Run `events schema init` to create the schemas directory.\n'))
    return false
  }

  const files = readdirSync(schemasDir).filter(f => f.endsWith('.json'))

  if (files.length === 0) {
    console.log(pc.yellow('\n  No schema files found in schemas directory.\n'))
    return true
  }

  console.log(pc.cyan('\n  Validating schemas...\n'))

  let hasErrors = false

  for (const file of files) {
    const filepath = join(schemasDir, file)
    try {
      const content = readFileSync(filepath, 'utf-8')
      const parsed = JSON.parse(content)
      const result = JsonSchemaValidator.safeParse(parsed)

      if (result.success) {
        console.log(pc.green(`  ${pc.bold('PASS')} ${file}`))
      } else {
        console.log(pc.red(`  ${pc.bold('FAIL')} ${file}`))
        for (const error of result.error.errors) {
          console.log(pc.dim(`       ${error.path.join('.')}: ${error.message}`))
        }
        hasErrors = true
      }
    } catch (err) {
      console.log(pc.red(`  ${pc.bold('FAIL')} ${file}`))
      console.log(pc.dim(`       ${err instanceof Error ? err.message : 'Unknown error'}`))
      hasErrors = true
    }
  }

  console.log()

  if (hasErrors) {
    console.log(pc.red('  Some schemas have validation errors.\n'))
    return false
  }

  console.log(pc.green('  All schemas are valid!\n'))
  return true
}

export function schemaGenerate(options: SchemaOptions = {}): void {
  const schemasDir = resolve(process.cwd(), options.dir || 'schemas')

  if (!existsSync(schemasDir)) {
    console.log(pc.red(`\n  Error: Schemas directory not found: ${schemasDir}`))
    console.log(pc.dim('  Run `events schema init` to create the schemas directory.\n'))
    return
  }

  const files = readdirSync(schemasDir).filter(f => f.endsWith('.json'))

  if (files.length === 0) {
    console.log(pc.yellow('\n  No schema files found in schemas directory.\n'))
    return
  }

  console.log(pc.cyan('\n  Generating TypeScript types from schemas...\n'))

  const types: string[] = [
    '// Auto-generated from JSON schemas - do not edit manually',
    '// Generated by @dotdo/events-cli',
    '',
  ]

  for (const file of files) {
    const filepath = join(schemasDir, file)
    try {
      const content = readFileSync(filepath, 'utf-8')
      const schema = JSON.parse(content)

      const typeName = basename(file, '.json')
        .split('-')
        .map(p => p.charAt(0).toUpperCase() + p.slice(1))
        .join('')

      types.push(`/** ${schema.description || typeName} */`)
      types.push(`export interface ${typeName} {`)

      if (schema.properties) {
        const required = new Set(schema.required || [])
        for (const [key, prop] of Object.entries(schema.properties)) {
          const propSchema = prop as Record<string, unknown>
          const isRequired = required.has(key)
          const tsType = jsonSchemaTypeToTs(propSchema)
          const optional = isRequired ? '' : '?'
          if (propSchema.description) {
            types.push(`  /** ${propSchema.description} */`)
          }
          types.push(`  ${key}${optional}: ${tsType}`)
        }
      }

      types.push('}')
      types.push('')

      console.log(pc.dim(`  Generated ${typeName} from ${file}`))
    } catch (err) {
      console.log(pc.yellow(`  Skipped ${file}: ${err instanceof Error ? err.message : 'Parse error'}`))
    }
  }

  const outputPath = join(process.cwd(), 'src', 'schema-types.ts')
  const srcDir = join(process.cwd(), 'src')

  if (!existsSync(srcDir)) {
    mkdirSync(srcDir, { recursive: true })
  }

  writeFileSync(outputPath, types.join('\n'))
  console.log(pc.green(`\n  Generated types written to ${outputPath}\n`))
}

function jsonSchemaTypeToTs(schema: Record<string, unknown>): string {
  if (schema.const !== undefined) {
    return typeof schema.const === 'string' ? `'${schema.const}'` : String(schema.const)
  }

  const type = schema.type as string | undefined

  switch (type) {
    case 'string':
      return 'string'
    case 'number':
    case 'integer':
      return 'number'
    case 'boolean':
      return 'boolean'
    case 'null':
      return 'null'
    case 'array':
      const items = schema.items as Record<string, unknown> | undefined
      return items ? `Array<${jsonSchemaTypeToTs(items)}>` : 'unknown[]'
    case 'object':
      if (schema.properties) {
        return 'Record<string, unknown>' // Simplified for nested objects
      }
      return 'Record<string, unknown>'
    default:
      return 'unknown'
  }
}

export const schemaCommand = new Command('schema')
  .description('Manage event schemas')

schemaCommand
  .command('init')
  .description('Initialize schemas directory with example schemas')
  .option('-d, --dir <directory>', 'Schemas directory (default: schemas)')
  .action((opts) => {
    schemaInit(opts)
  })

schemaCommand
  .command('validate')
  .description('Validate all schema files')
  .option('-d, --dir <directory>', 'Schemas directory (default: schemas)')
  .action((opts) => {
    const valid = schemaValidate(opts)
    process.exit(valid ? 0 : 1)
  })

schemaCommand
  .command('generate')
  .description('Generate TypeScript types from schemas')
  .option('-d, --dir <directory>', 'Schemas directory (default: schemas)')
  .action((opts) => {
    schemaGenerate(opts)
  })
