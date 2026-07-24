import { writeFileSync } from "node:fs";

import YAML from "yaml";

import { spec } from "./generate-spec.js";

const outputPath = "asyncapi.yaml";
writeFileSync(outputPath, YAML.stringify(spec));

console.log(`✅ Generated AsyncAPI spec: ${outputPath}`);
console.log(`   Channels: ${Object.keys(spec.channels ?? {}).length}`);
console.log(`   Operations: ${Object.keys(spec.operations ?? {}).length}`);
