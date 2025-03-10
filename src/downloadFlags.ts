import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { extname, join } from "node:path";
import { FLAG } from "./flag";

async function downloadFlags() {
  // Create flags directory if it doesn't exist
  const flagsDir = join(__dirname, "flags2");
  if (!existsSync(flagsDir)) {
    mkdirSync(flagsDir);
  }

  // Download flags
  // for (const country of CountryCode) {
  for (const country of Object.entries(FLAG)) {
    try {
      const response = await fetch(country[1]);
      const buffer = await response.arrayBuffer();

      // Save flag with country code as filename
      const filePath = join(
        flagsDir,
        `${country[0].toLowerCase()}${extname(country[1].toString())}`
      );
      writeFileSync(filePath, Buffer.from(buffer));

      console.log(`Downloaded flag for ${country[0]}: ${country[1]}`);
    } catch (error) {
      console.error(`Error downloading flag for ${country[0]}:`, error);
    }
  }
}

// Run the download
downloadFlags()
  .then(() => {
    console.log("All downloads completed");
  })
  .catch((error) => {
    console.error("Download failed:", error);
  });
