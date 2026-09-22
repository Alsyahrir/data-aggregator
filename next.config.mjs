/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  rewrites: async () => {
    // In local development, forward API calls to the local FastAPI backend
    return [
      {
        source: '/api/:path*',
        destination:
          process.env.NODE_ENV === 'development'
            ? 'http://127.0.0.1:8000/api/:path*'
            : '/api/:path*',
      },
    ];
  },
};

export default nextConfig;
