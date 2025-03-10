import { createWriteStream } from "node:fs";
import {
  createCSVStream,
  FieldConfig,
  generateCSVFile,
} from "./generate-csv.core";

// First, install the required package:
// npm install @faker-js/faker

const csvConfig: FieldConfig[] = [
  {
    name: "First Name",
    type: "name",
  },
  {
    name: "Last Name",
    type: "name",
  },
  {
    name: "Email",
    type: "email",
  },
  {
    name: "Accepts Email Marketing",
    type: "options",
    options: ["yes", "no"],
  },
  {
    name: "Default Address Company",
    type: "company",
    allowNull: true,
    nullChance: 0.5,
  },
  {
    name: "Default Address Address1",
    type: "address",
    allowNull: true,
    nullChance: 0.5,
  },
  {
    name: "Default Address Address2",
    type: "address",
    allowNull: true,
    nullChance: 0.5,
  },
  {
    name: "Default Address City",
    type: "string",
    allowNull: true,
    nullChance: 1,
  },
  {
    name: "Default Address Province Code",
    type: "string",
    allowNull: true,
    nullChance: 1,
  },
  {
    name: "Default Address Country Code",
    type: "string",
    allowNull: true,
    nullChance: 1,
  },
  {
    name: "Default Address Zip",
    type: "string",
    allowNull: true,
    nullChance: 1,
  },
  {
    name: "Default Address Phone",
    type: "string",
    allowNull: true,
    nullChance: 1,
  },
  {
    name: "Phone",
    type: "phone",
  },
  {
    name: "Accepts SMS Marketing",
    type: "options",
    options: ["yes", "no"],
  },
  {
    name: "Tags",
    type: "options",
    options: ["cc1", "cc2", "cc3", "cc4", "cc5"],
    allowNull: true,
  },
  {
    name: "Note",
    type: "string",
    allowNull: true,
    nullChance: 0.5,
  },
  {
    name: "Tax Exempt",
    type: "options",
    options: ["yes", "no"],
  },
];

// Example 1: Generate CSV directly to file (most memory efficient)
async function example1() {
  console.time("File generation");
  await generateCSVFile("large-dataset.csv", csvConfig, 130000, {
    highWaterMark: 64 * 1024, // 64KB buffer for better performance
  });
  console.timeEnd("File generation");
}

// Example 2: Create a stream and pipe it (flexible approach)
function example2() {
  console.time("Stream piping");
  const csvStream = createCSVStream(csvConfig, 130000);
  const fileStream = createWriteStream("streamed-dataset.csv");

  csvStream.pipe(fileStream);

  fileStream.on("finish", () => {
    console.timeEnd("Stream piping");
  });
}

// Run examples
async function run() {
  await example1();
  example2();
}

run().catch(console.error);
