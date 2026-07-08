/**
 * Static-export config for the flashbook evidence dashboard.
 *
 * NEXT_PUBLIC_BASE_PATH controls the deploy prefix:
 *   - ""            → root deploy (Vercel, local `npx serve out`)
 *   - "/flashbook"  → GitHub Pages project site
 *
 * The same env var is inlined into client code so data fetches
 * (`${basePath}/data/*.json`) resolve relative to the deploy prefix.
 */
const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? "";

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "export",
  ...(basePath ? { basePath, assetPrefix: basePath } : {}),
  images: { unoptimized: true },
};

export default nextConfig;
