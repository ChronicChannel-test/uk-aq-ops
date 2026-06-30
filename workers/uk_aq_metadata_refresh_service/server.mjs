import { main } from "./core.mjs";

main().catch((error) => {
  console.error(JSON.stringify({ severity: "ERROR", event: "metadata_refresh_service_failed", timestamp: new Date().toISOString(), error: error instanceof Error ? error.message : String(error) }));
  process.exitCode = 1;
});
