import process from "node:process";

export function sourceApiUrl(path: string) {
  const base = process.env.SOURCE_API_BASE_URL?.trim();
  if (!base) throw new Error("The source service is not configured.");

  const url = new URL(base);
  const local = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  const localDevelopment =
    url.protocol === "http:" && local && process.env.NODE_ENV !== "production";

  if (
    url.username ||
    url.password ||
    url.href.includes("?") ||
    url.href.includes("#")
  )
    throw new Error("Invalid source service URL.");
  if (url.protocol !== "https:" && !localDevelopment)
    throw new Error("The source service requires HTTPS.");

  url.pathname = url.pathname.replace(/\/+$/, "") + path;
  return url;
}
