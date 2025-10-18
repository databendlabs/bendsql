/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  trailingSlash: false,
  output: 'export',
  distDir: 'build',
  images: {
    unoptimized: true
  },
  // Generate fixed filenames to reduce git changes
  webpack: (config, { isServer }) => {
    if (!isServer) {
      // Use fixed filenames instead of content hashes
      config.output.filename = 'static/js/[name].js'
      config.output.chunkFilename = 'static/js/[name].js'
      
      // Ensure CSS filenames are also fixed
      const miniCssExtractPlugin = config.plugins.find(
        plugin => plugin.constructor.name === 'MiniCssExtractPlugin'
      )
      if (miniCssExtractPlugin) {
        miniCssExtractPlugin.options.filename = 'static/css/[name].css'
        miniCssExtractPlugin.options.chunkFilename = 'static/css/[name].css'
      }

      // Disable dynamic chunk naming in code splitting
      config.optimization = {
        ...config.optimization,
        splitChunks: {
          ...config.optimization.splitChunks,
          cacheGroups: {
            ...config.optimization.splitChunks.cacheGroups,
            default: false,
            vendors: false,
            // Merge all vendor code into one file
            vendor: {
              name: 'vendor',
              chunks: 'all',
              test: /node_modules/,
              filename: 'static/js/vendor.js'
            },
            // Merge all shared code
            common: {
              name: 'common',
              chunks: 'all',
              minChunks: 2,
              filename: 'static/js/common.js'
            }
          }
        }
      }
    }
    return config
  },
  // Ensure build ID is stable
  generateBuildId: async () => {
    return 'stable-build-id'
  }
}

module.exports = nextConfig