import { readFile, writeFile, access } from "node:fs/promises";
import { resolve } from "node:path";
import process from "node:process";

const root = process.cwd();
const read = (path) => readFile(resolve(root, path), "utf8");
const write = (path, text) => writeFile(resolve(root, path), text);

// Validate the project root before touching configuration. Preserve credentials,
// current tunnel URLs, scopes, webhook versions and all other package settings.
for (const path of [
  "package.json",
  "package-lock.json",
  "shopify.app.toml",
  "shopify.web.toml",
])
  await access(resolve(root, path));
const pkg = JSON.parse(await read("package.json"));
const lock = JSON.parse(await read("package-lock.json"));
let config = await read("shopify.app.toml");
if (!/^name[\t ]*=[\t ]*"[^"\n]*"[\t ]*$/m.test(config))
  throw new Error(
    "Cannot locate the app name in shopify.app.toml; no configuration was changed.",
  );
pkg.name = "smart-store-sync";
lock.name = "smart-store-sync";
if (lock.packages?.[""]) lock.packages[""].name = "smart-store-sync";
config = config.replace(
  /^name[\t ]*=[\t ]*"[^"\n]*"[\t ]*$/m,
  'name = "Smart Store Sync"',
);
if (/^handle[\t ]*=/m.test(config))
  config = config.replace(
    /^handle[\t ]*=[\t ]*"[^"\n]*"[\t ]*$/m,
    'handle = "smart-store-sync"',
  );
else
  config = config.replace(
    'name = "Smart Store Sync"',
    'name = "Smart Store Sync"\nhandle = "smart-store-sync"',
  );
config = config
  .replace('name = "Demo Source Info"', 'name = "Smart Store Sync source info"')
  .replace(
    'description = "Tracks products created by the Shopify app template for development"',
    'description = "Source information managed by Smart Store Sync"',
  )
  .replace('name = "Example"', 'name = "Smart Store Sync record"')
  .replace(
    'description = "An example metaobject definition created by this template"',
    'description = "Structured record managed by Smart Store Sync"',
  );
await write("package.json", JSON.stringify(pkg, null, 2) + "\n");
await write("package-lock.json", JSON.stringify(lock, null, 2) + "\n");
await write("shopify.app.toml", config);
await write(
  "shopify.web.toml",
  (await read("shopify.web.toml")).replace(
    /^name[\t ]*=[\t ]*"[^"\n]*"[\t ]*$/m,
    'name = "Smart Store Sync"',
  ),
);
const inventory = "app/services/inventory.server.ts";
await write(
  inventory,
  (await read(inventory)).replaceAll(
    "gid://cartpe-store-sync/",
    "gid://smart-store-sync/",
  ),
);
const agents = "AGENTS.md";
try {
  const text = await read(agents);
  await write(
    agents,
    text.replace(
      "This app is scaffolded from a Shopify app template. See the README for framework-specific details.",
      "Smart Store Sync is a Shopify app for automated source-catalog synchronization. See the README for setup and operation.",
    ),
  );
} catch (error) {
  if (error.code !== "ENOENT") throw error;
}
console.log(
  "Smart Store Sync branding applied. Existing client ID, URLs and integration settings preserved.",
);
console.log(
  "Run npm run lint -- --no-cache, npm run typecheck, and shopify app config validate --json.",
);
