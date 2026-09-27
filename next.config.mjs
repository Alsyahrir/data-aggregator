/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Next 16 writes AGENTS.md and CLAUDE.md into the repo root on every dev
  // run. This project doesn't use them, and regenerating untracked files on
  // each start is noise (and would clobber a hand-written CLAUDE.md).
  agentRules: false,
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
