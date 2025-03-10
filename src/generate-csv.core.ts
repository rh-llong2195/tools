import { faker } from "@faker-js/faker";
import { createWriteStream } from "node:fs";
import { promisify } from "node:util";
import { Readable, pipeline } from "stream";

const pipelineAsync = promisify(pipeline);

/**
 * Field configuration for CSV generation
 */
export interface FieldConfig {
  name: string;
  type:
    | "string"
    | "number"
    | "date"
    | "boolean"
    | "email"
    | "name"
    | "address"
    | "phone"
    | "company"
    | "uuid"
    | "options"
    | "lorem"
    | "custom";
  length?: number; // For strings/lorem - character length
  min?: number; // For numbers - minimum value
  max?: number; // For numbers - maximum value
  decimals?: number; // For numbers - decimal places
  options?: string[]; // For options type - possible values
  allowNull?: boolean; // Whether field can be null
  nullChance?: number; // Chance of being null (0-1)
  past?: boolean; // For dates - generate past dates
  future?: boolean; // For dates - generate future dates
  dateFormat?: string; // Date format (default: ISO)
  fakerMethod?: string; // Specific faker method to use
  customGenerator?: () => any; // Custom generator function
}

/**
 * Options for CSV generation
 */
interface CSVOptions {
  delimiter?: string;
  includeHeader?: boolean;
  quoteStrings?: boolean;
  locale?: string;
  highWaterMark?: number; // Buffer size
  encoding?: BufferEncoding;
}

/**
 * Generates a value based on field configuration
 */
function generateValue(field: FieldConfig): any {
  // Check for null value
  if (field.allowNull && field.nullChance && Math.random() < field.nullChance) {
    return "";
  }

  let value: any;

  switch (field.type) {
    case "string":
      value = faker.string.alphanumeric(field.length || 10);
      break;

    case "lorem":
      value =
        field.length && field.length < 20
          ? faker.lorem.word(field.length)
          : faker.lorem.sentence(
              field.length ? Math.ceil(field.length / 10) : 5
            );
      break;

    case "number":
      value = faker.number.float({
        min: field.min !== undefined ? field.min : 0,
        max: field.max !== undefined ? field.max : 1000,
        fractionDigits:
          field.decimals !== undefined ? Math.pow(0.1, field.decimals) : 1,
      });
      break;

    case "date":
      if (field.past) {
        value = faker.date.past().toISOString();
      } else if (field.future) {
        value = faker.date.future().toISOString();
      } else {
        value = faker.date.anytime().toISOString();
      }

      if (field.dateFormat) {
        const date = new Date(value);
        value = field.dateFormat
          .replace("YYYY", date.getFullYear().toString())
          .replace("MM", String(date.getMonth() + 1).padStart(2, "0"))
          .replace("DD", String(date.getDate()).padStart(2, "0"));
      }
      break;

    case "boolean":
      value = faker.datatype.boolean();
      break;

    case "email":
      value = faker.internet.email();
      break;

    case "name":
      value = faker.person.fullName();
      break;

    case "address":
      value = faker.location.streetAddress();
      break;

    case "phone":
      value = faker.phone.number();
      break;

    case "company":
      value = faker.company.name();
      break;

    case "uuid":
      value = faker.string.uuid();
      break;

    case "options":
      if (!field.options || field.options.length === 0) {
        value = "";
      } else {
        value = faker.helpers.arrayElement(field.options);
      }
      break;

    case "custom":
      if (field.customGenerator) {
        value = field.customGenerator();
      } else if (field.fakerMethod) {
        try {
          const [namespace, method] = field.fakerMethod.split(".");
          value = (faker as any)[namespace][method]();
        } catch (e) {
          value = "";
        }
      }
      break;

    default:
      value = "";
  }

  return value;
}

/**
 * Format a value for CSV inclusion
 */
function formatValueForCSV(value: any, quoteStrings: boolean): string {
  if (typeof value === "string" && quoteStrings) {
    // Escape double quotes in the string and wrap in quotes
    return `"${value.replace(/"/g, '""')}"`;
  } else if (typeof value === "boolean" || typeof value === "number") {
    return value.toString();
  } else if (value === null || value === undefined) {
    return "";
  } else {
    return quoteStrings ? `"${value}"` : String(value);
  }
}

/**
 * Create a readable stream that generates CSV data
 */
class CSVGeneratorStream extends Readable {
  private recordsLeft: number;
  private readonly config: FieldConfig[];
  private readonly options: Required<CSVOptions>;
  private headerSent: boolean = false;

  constructor(
    config: FieldConfig[],
    numRecords: number = 100,
    options: CSVOptions = {}
  ) {
    const defaultOptions = {
      delimiter: ",",
      includeHeader: true,
      quoteStrings: true,
      locale: "en",
      highWaterMark: 16 * 1024, // 16KB buffer
      encoding: "utf8" as BufferEncoding,
    };

    super({
      highWaterMark: options.highWaterMark || defaultOptions.highWaterMark,
      encoding: options.encoding || defaultOptions.encoding,
    });

    this.config = config;
    this.recordsLeft = numRecords;
    this.options = { ...defaultOptions, ...options };
  }

  _read(): void {
    // Send header first if needed
    if (this.options.includeHeader && !this.headerSent) {
      const header =
        this.config
          .map((field) =>
            this.options.quoteStrings ? `"${field.name}"` : field.name
          )
          .join(this.options.delimiter) + "\n";
      this.push(Buffer.from(header, this.options.encoding));
      this.headerSent = true;
      return;
    }

    // End stream if no more records to generate
    if (this.recordsLeft <= 0) {
      this.push(null);
      return;
    }

    // Generate batch of records (up to buffer size)
    const batchSize = Math.min(100, this.recordsLeft);
    let batchData = "";

    for (let i = 0; i < batchSize; i++) {
      const record = this.config
        .map((field) =>
          formatValueForCSV(generateValue(field), this.options.quoteStrings)
        )
        .join(this.options.delimiter);

      batchData += record + "\n";
    }

    this.recordsLeft -= batchSize;
    this.push(Buffer.from(batchData, this.options.encoding));
  }
}

/**
 * Generate CSV data as a stream
 */
export function createCSVStream(
  config: FieldConfig[],
  numRecords: number = 100,
  options: CSVOptions = {}
): Readable {
  return new CSVGeneratorStream(config, numRecords, options);
}

/**
 * Generate CSV file using streams
 */
export async function generateCSVFile(
  filePath: string,
  config: FieldConfig[],
  numRecords: number = 100,
  options: CSVOptions = {}
): Promise<void> {
  const csvStream = createCSVStream(config, numRecords, options);
  const fileStream = createWriteStream(filePath);

  await pipelineAsync(csvStream, fileStream);
}

/**
 * Generate CSV string (for smaller datasets)
 */
export function generateCSV(
  config: FieldConfig[],
  numRecords: number = 100,
  options: CSVOptions = {}
): Promise<string> {
  return new Promise((resolve, reject) => {
    const csvStream = createCSVStream(config, numRecords, options);
    const chunks: Buffer[] = [];

    csvStream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    csvStream.on("error", reject);
    csvStream.on("end", () => {
      resolve(Buffer.concat(chunks).toString(options.encoding || "utf8"));
    });
  });
}
