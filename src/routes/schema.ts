/**
 * Schema Registry route handlers
 *
 * Provides HTTP endpoints for managing JSON schemas for event validation:
 * - POST /schemas - Register/update a schema
 * - GET /schemas - List schemas
 * - GET /schemas/:namespace/:eventType - Get a specific schema
 * - DELETE /schemas/:namespace/:eventType - Delete a schema
 * - PUT /schemas/namespace/:namespace/config - Configure namespace validation settings
 * - GET /schemas/namespace/:namespace/config - Get namespace configuration
 */

import type { Env } from '../env'
import type { JsonSchema, SchemaRegistration, ValidationError, NamespaceConfig } from '../../core/src/schema-registry'
import { corsHeaders } from '../utils'
import { successResponse, errorResponse, badRequest, notFound, notConfigured, internalError, ErrorCodes } from '../utils/response'
import { logger, logError } from '../logger'

const log = logger.child({ component: 'SchemaRoutes' })

/**
 * Type for validation results returned by the schema registry
 */
interface BatchValidationResult {
  valid: boolean
  results: (ValidationResult & { index: number; eventType: string })[]
}

interface ValidationResult {
  valid: boolean
  errors: ValidationError[]
}

// ============================================================================
// Schema Registration
// ============================================================================

/**
 * POST /schemas - Register or update a schema
 */
export async function handleRegisterSchema(request: Request, env: Env): Promise<Response> {
  if (!env.SCHEMA_REGISTRY) {
    return notConfigured('Schema registry not configured', { headers: corsHeaders() })
  }

  let body: {
    eventType: string
    namespace?: string
    schema: JsonSchema
    validationEnabled?: boolean
    strictMode?: boolean
    description?: string
  }

  try {
    body = await request.json()
  } catch {
    return badRequest('Invalid JSON', ErrorCodes.INVALID_JSON, { headers: corsHeaders() })
  }

  // Validate required fields
  if (!body.eventType || typeof body.eventType !== 'string') {
    return badRequest('eventType is required and must be a string', ErrorCodes.MISSING_FIELD, { headers: corsHeaders() })
  }

  if (!body.schema || typeof body.schema !== 'object') {
    return badRequest('schema is required and must be a valid JSON Schema object', ErrorCodes.MISSING_FIELD, { headers: corsHeaders() })
  }

  const namespace = body.namespace ?? 'default'

  try {
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    const result = await registry.registerSchema({
      eventType: body.eventType,
      namespace,
      schema: body.schema,
      validationEnabled: body.validationEnabled,
      strictMode: body.strictMode,
      description: body.description,
    })

    if (!result.ok) {
      return badRequest(result.error || 'Registration failed', ErrorCodes.INVALID_REQUEST, { headers: corsHeaders() })
    }

    return successResponse(
      {
        ok: true,
        schemaId: result.schemaId,
        version: result.version,
        namespace,
        eventType: body.eventType,
      },
      { status: 201, headers: corsHeaders() }
    )
  } catch (err) {
    logError(log, 'Registration error', err)
    return internalError('Failed to register schema', { headers: corsHeaders() })
  }
}

// ============================================================================
// Schema Retrieval
// ============================================================================

/**
 * GET /schemas - List all schemas
 * Query params: namespace, limit, offset
 */
export async function handleListSchemas(request: Request, env: Env): Promise<Response> {
  if (!env.SCHEMA_REGISTRY) {
    return Response.json(
      { error: 'Schema registry not configured' },
      { status: 501, headers: corsHeaders() }
    )
  }

  const url = new URL(request.url)
  const namespace = url.searchParams.get('namespace') ?? undefined
  const limit = parseInt(url.searchParams.get('limit') ?? '100', 10)
  const offset = parseInt(url.searchParams.get('offset') ?? '0', 10)

  try {
    // Get schemas from default namespace registry, which can list all
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace ?? 'default')
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    const schemas = await registry.listSchemas({ namespace, limit, offset }) as SchemaRegistration[]

    return Response.json(
      {
        schemas,
        count: schemas.length,
        limit,
        offset,
      },
      { headers: corsHeaders() }
    )
  } catch (err) {
    logError(log, 'List error', err)
    return Response.json(
      { error: 'Failed to list schemas' },
      { status: 500, headers: corsHeaders() }
    )
  }
}

/**
 * GET /schemas/:namespace/:eventType - Get a specific schema
 */
export async function handleGetSchema(request: Request, env: Env, params: { namespace: string; eventType: string }): Promise<Response> {
  if (!env.SCHEMA_REGISTRY) {
    return Response.json(
      { error: 'Schema registry not configured' },
      { status: 501, headers: corsHeaders() }
    )
  }

  const { namespace, eventType } = params

  try {
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    const schema = await registry.getSchema({ eventType, namespace })

    if (!schema) {
      return Response.json(
        { error: 'Schema not found' },
        { status: 404, headers: corsHeaders() }
      )
    }

    return Response.json(schema, { headers: corsHeaders() })
  } catch (err) {
    logError(log, 'Get error', err)
    return Response.json(
      { error: 'Failed to get schema' },
      { status: 500, headers: corsHeaders() }
    )
  }
}

/**
 * GET /schemas/:namespace/:eventType/history - Get schema version history
 */
export async function handleGetSchemaHistory(request: Request, env: Env, params: { namespace: string; eventType: string }): Promise<Response> {
  if (!env.SCHEMA_REGISTRY) {
    return Response.json(
      { error: 'Schema registry not configured' },
      { status: 501, headers: corsHeaders() }
    )
  }

  const { namespace, eventType } = params

  try {
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    const history = await registry.getSchemaHistory({ eventType, namespace }) as { version: number; schema: JsonSchema; createdAt: number }[]

    return Response.json(
      { history, count: history.length },
      { headers: corsHeaders() }
    )
  } catch (err) {
    logError(log, 'History error', err)
    return Response.json(
      { error: 'Failed to get schema history' },
      { status: 500, headers: corsHeaders() }
    )
  }
}

// ============================================================================
// Schema Deletion
// ============================================================================

/**
 * DELETE /schemas/:namespace/:eventType - Delete a schema
 */
export async function handleDeleteSchema(request: Request, env: Env, params: { namespace: string; eventType: string }): Promise<Response> {
  if (!env.SCHEMA_REGISTRY) {
    return Response.json(
      { error: 'Schema registry not configured' },
      { status: 501, headers: corsHeaders() }
    )
  }

  const { namespace, eventType } = params

  try {
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    const result = await registry.deleteSchema({ eventType, namespace })

    if (!result.deleted) {
      return Response.json(
        { error: 'Schema not found' },
        { status: 404, headers: corsHeaders() }
      )
    }

    return Response.json(
      { ok: true, deleted: true },
      { headers: corsHeaders() }
    )
  } catch (err) {
    logError(log, 'Delete error', err)
    return Response.json(
      { error: 'Failed to delete schema' },
      { status: 500, headers: corsHeaders() }
    )
  }
}

// ============================================================================
// Namespace Configuration
// ============================================================================

/**
 * PUT /schemas/namespace/:namespace/config - Configure namespace validation settings
 */
export async function handleConfigureNamespace(request: Request, env: Env, params: { namespace: string }): Promise<Response> {
  if (!env.SCHEMA_REGISTRY) {
    return Response.json(
      { error: 'Schema registry not configured' },
      { status: 501, headers: corsHeaders() }
    )
  }

  let body: {
    validationEnabled?: boolean
    strictMode?: boolean
  }

  try {
    body = await request.json()
  } catch {
    return Response.json(
      { error: 'Invalid JSON' },
      { status: 400, headers: corsHeaders() }
    )
  }

  const { namespace } = params

  try {
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    await registry.configureNamespace({
      namespace,
      validationEnabled: body.validationEnabled,
      strictMode: body.strictMode,
    })

    return Response.json(
      { ok: true, namespace },
      { headers: corsHeaders() }
    )
  } catch (err) {
    logError(log, 'Configure namespace error', err)
    return Response.json(
      { error: 'Failed to configure namespace' },
      { status: 500, headers: corsHeaders() }
    )
  }
}

/**
 * GET /schemas/namespace/:namespace/config - Get namespace configuration
 */
export async function handleGetNamespaceConfig(request: Request, env: Env, params: { namespace: string }): Promise<Response> {
  if (!env.SCHEMA_REGISTRY) {
    return Response.json(
      { error: 'Schema registry not configured' },
      { status: 501, headers: corsHeaders() }
    )
  }

  const { namespace } = params

  try {
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    const config = await registry.getNamespaceConfig(namespace)

    if (!config) {
      // Return default config if not explicitly configured
      return Response.json(
        {
          namespace,
          validationEnabled: true,
          strictMode: false,
          configured: false,
        },
        { headers: corsHeaders() }
      )
    }

    return Response.json(
      { ...config, configured: true },
      { headers: corsHeaders() }
    )
  } catch (err) {
    logError(log, 'Get namespace config error', err)
    return Response.json(
      { error: 'Failed to get namespace config' },
      { status: 500, headers: corsHeaders() }
    )
  }
}

// ============================================================================
// Event Validation (Testing)
// ============================================================================

/**
 * POST /schemas/validate - Validate events against schemas (for testing)
 */
export async function handleValidateEvents(request: Request, env: Env): Promise<Response> {
  if (!env.SCHEMA_REGISTRY) {
    return Response.json(
      { error: 'Schema registry not configured' },
      { status: 501, headers: corsHeaders() }
    )
  }

  let body: {
    events: { type: string; [key: string]: unknown }[]
    namespace?: string
  }

  try {
    body = await request.json()
  } catch {
    return Response.json(
      { error: 'Invalid JSON' },
      { status: 400, headers: corsHeaders() }
    )
  }

  if (!Array.isArray(body.events) || body.events.length === 0) {
    return Response.json(
      { error: 'events must be a non-empty array' },
      { status: 400, headers: corsHeaders() }
    )
  }

  const namespace = body.namespace ?? 'default'

  try {
    const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
    const registry = env.SCHEMA_REGISTRY.get(registryId)

    const result = await registry.validateEvents(body.events, namespace) as BatchValidationResult

    return Response.json(
      {
        valid: result.valid,
        totalEvents: body.events.length,
        invalidCount: result.results.length,
        results: result.results,
      },
      { headers: corsHeaders() }
    )
  } catch (err) {
    logError(log, 'Validation error', err)
    return Response.json(
      { error: 'Failed to validate events' },
      { status: 500, headers: corsHeaders() }
    )
  }
}
