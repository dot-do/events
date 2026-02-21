import type { BaseEvent } from './base.js'

// EPCIS 2.0 event type discriminators
export type EpcisEventType =
  | 'epcis.object'
  | 'epcis.aggregation'
  | 'epcis.transaction'
  | 'epcis.transformation'
  | 'epcis.association'

// CBV Business Steps (Core Business Vocabulary)
export type BizStep =
  | 'accepting'
  | 'arriving'
  | 'assembling'
  | 'collecting'
  | 'commissioning'
  | 'consigning'
  | 'creating_class_instance'
  | 'cycle_counting'
  | 'decommissioning'
  | 'departing'
  | 'destroying'
  | 'disassembling'
  | 'dispensing'
  | 'encoding'
  | 'entering_exiting'
  | 'holding'
  | 'inspecting'
  | 'installing'
  | 'killing'
  | 'loading'
  | 'other'
  | 'packing'
  | 'picking'
  | 'receiving'
  | 'removing'
  | 'repackaging'
  | 'repairing'
  | 'replacing'
  | 'reserving'
  | 'retail_selling'
  | 'sampling'
  | 'sensor_reporting'
  | 'shipping'
  | 'staging_outbound'
  | 'stock_taking'
  | 'stocking'
  | 'storing'
  | 'transporting'
  | 'unloading'
  | 'unpacking'
  | 'void_shipping'
  | string // allow custom extensions

// CBV Dispositions
export type Disposition =
  | 'active'
  | 'available'
  | 'completeness_verified'
  | 'completeness_inferred'
  | 'conformant'
  | 'container_closed'
  | 'container_open'
  | 'damaged'
  | 'destroyed'
  | 'dispensed'
  | 'disposed'
  | 'encoded'
  | 'expired'
  | 'in_progress'
  | 'in_transit'
  | 'inactive'
  | 'mismatch_instance'
  | 'mismatch_class'
  | 'mismatch_quantity'
  | 'needs_replacement'
  | 'no_pedigree_match'
  | 'non_conformant'
  | 'non_sellable_other'
  | 'partially_dispensed'
  | 'recalled'
  | 'reserved'
  | 'retail_sold'
  | 'returned'
  | 'sellable_accessible'
  | 'sellable_not_accessible'
  | 'stolen'
  | 'unavailable'
  | 'unknown'
  | string

// EPCIS Action
export type EpcisAction = 'ADD' | 'DELETE' | 'OBSERVE'

// Quantity element (class-level identification)
export interface QuantityElement {
  epcClass: string
  quantity?: number
  uom?: string // UN/CEFACT unit of measure code
}

// Business Transaction
export interface BizTransaction {
  type: string // 'po', 'inv', 'desadv', 'bol', etc.
  bizTransaction: string
}

// Source or Destination
export interface SourceDest {
  type: 'location' | 'owning_party' | 'possessing_party' | string
  source?: string
  destination?: string
}

// Sensor Report (EPCIS 2.0)
export interface SensorReport {
  type?: string // gs1:Temperature, gs1:Humidity, etc.
  value?: number
  uom?: string
  minValue?: number
  maxValue?: number
  meanValue?: number
  sDev?: number
  time?: string
  component?: string
  stringValue?: string
  booleanValue?: boolean
  hexBinaryValue?: string
  uriValue?: string
}

// Sensor Element (EPCIS 2.0)
export interface SensorElement {
  sensorMetadata?: {
    time?: string
    deviceID?: string
    deviceMetadata?: string
    rawData?: string
    startTime?: string
    endTime?: string
    dataProcessingMethod?: string
    bizRules?: string
  }
  sensorReport: SensorReport[]
}

// Persistent Disposition (EPCIS 2.0)
export interface PersistentDisposition {
  set?: Disposition[]
  unset?: Disposition[]
}

// Error Declaration
export interface ErrorDeclaration {
  declarationTime: string
  reason?: string
  correctiveEventIDs?: string[]
}

// Common fields shared by all EPCIS event types
export interface EpcisCommonData {
  eventId?: string // eventID (SHA-256 NI URI)
  action?: EpcisAction
  bizStep?: BizStep
  bizStepUri?: string // full CBV URI or custom
  disposition?: Disposition
  dispositionUri?: string
  readPoint?: string // SGLN URI
  bizLocation?: string // SGLN URI
  bizTransactionList?: BizTransaction[]
  sourceList?: SourceDest[]
  destinationList?: SourceDest[]
  sensorElementList?: SensorElement[]
  persistentDisposition?: PersistentDisposition
  errorDeclaration?: ErrorDeclaration
  certificationInfo?: string
  eventTimeZoneOffset?: string
  ilmd?: Record<string, unknown>
}

// ObjectEvent data
export interface ObjectEventData extends EpcisCommonData {
  epcList?: string[]
  quantityList?: QuantityElement[]
}

export interface ObjectEvent extends BaseEvent<'epcis.object', ObjectEventData> {}

// AggregationEvent data
export interface AggregationEventData extends EpcisCommonData {
  parentID?: string
  childEPCs?: string[]
  childQuantityList?: QuantityElement[]
}

export interface AggregationEvent extends BaseEvent<'epcis.aggregation', AggregationEventData> {}

// TransactionEvent data
export interface TransactionEventData extends EpcisCommonData {
  parentID?: string
  epcList?: string[]
  quantityList?: QuantityElement[]
}

export interface TransactionEvent extends BaseEvent<'epcis.transaction', TransactionEventData> {}

// TransformationEvent data (no action field — transformation is irreversible)
export interface TransformationEventData extends Omit<EpcisCommonData, 'action'> {
  inputEPCList?: string[]
  inputQuantityList?: QuantityElement[]
  outputEPCList?: string[]
  outputQuantityList?: QuantityElement[]
  transformationID?: string
}

export interface TransformationEvent extends BaseEvent<'epcis.transformation', TransformationEventData> {}

// AssociationEvent data (EPCIS 2.0)
export interface AssociationEventData extends EpcisCommonData {
  parentID?: string
  childEPCs?: string[]
  childQuantityList?: QuantityElement[]
}

export interface AssociationEvent extends BaseEvent<'epcis.association', AssociationEventData> {}

// Union of all EPCIS events
export type EpcisEvent = ObjectEvent | AggregationEvent | TransactionEvent | TransformationEvent | AssociationEvent
