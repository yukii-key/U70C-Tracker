// Vercel serverless function: shared leader settings (ranks, notes, AOO teams...) for every device.
// Storage: Upstash Redis (add it from the Vercel dashboard -> Storage / Marketplace).
const crypto = require('crypto');
// finds the database variables whether or not Vercel added a custom prefix (e.g. STORAGE_KV_REST_API_URL)
const env = re => { const k = Object.keys(process.env).find(n => re.test(n)); return k ? process.env[k] : undefined; };
const URL_ = env(/(^|_)(KV_REST_API_URL|UPSTASH_REDIS_REST_URL)$/);
const TOK = env(/(^|_)(KV_REST_API_TOKEN|UPSTASH_REDIS_REST_TOKEN)$/);
const KEY = 'u70c:meta', TKEY = 'u70c:tok';
// atomic compare-and-set: only write when the stored version equals the version the client started from
const LUA = "local cur=redis.call('GET',KEYS[1]) local cv=0 if cur then cv=tonumber(cjson.decode(cur)['v']) or 0 end if cv~=tonumber(ARGV[1]) then return 0 end redis.call('SET',KEYS[1],ARGV[2]) return 1";
async function redis(cmd) {
  const r = await fetch(URL_, { method: 'POST', headers: { Authorization: 'Bearer ' + TOK }, body: JSON.stringify(cmd) });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
const sha = s => crypto.createHash('sha256').update(String(s)).digest('hex');
module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!URL_ || !TOK) return res.status(501).json({ error: 'Redis is not connected to this project yet' });
  try {
    if (req.method === 'GET') {
      // normal reads are cached for 30s at Vercel's edge, so many open tabs cost only a few database commands
      if (!(req.query && req.query.fresh)) res.setHeader('Cache-Control', 'public, s-maxage=30, stale-while-revalidate=60');
      const cur = await redis(['GET', KEY]);
      const o = cur ? JSON.parse(cur) : { v: 0, t: 0, meta: null };
      return res.status(200).json({ v: o.v, t: o.t, meta: o.meta });
    }
    if (req.method === 'POST') {
      let b = req.body; if (typeof b === 'string') b = JSON.parse(b);
      if (!b || typeof b.meta !== 'object' || !b.meta) return res.status(400).json({ error: 'meta missing' });
      const th = sha(b.token || '');
      let stored = await redis(['GET', TKEY]);
      if (!stored) { // first ever write: needs the setup word you chose in Vercel
        if (!process.env.SYNC_SETUP || b.setup !== process.env.SYNC_SETUP) return res.status(401).json({ error: 'Wrong setup word' });
        await redis(['SET', TKEY, th, 'NX']);
        stored = await redis(['GET', TKEY]);
      }
      if (stored !== th) return res.status(401).json({ error: 'Not authorised – sign in as a leader again' });
      const ifv = Number(b.ifv) || 0, nv = ifv + 1;
      const out = JSON.stringify({ v: nv, t: Date.now(), meta: b.meta });
      if (out.length > 950000) return res.status(413).json({ error: 'Leader data is too large to sync' });
      const ok = await redis(['EVAL', LUA, '1', KEY, String(ifv), out]);
      if (Number(ok) !== 1) return res.status(409).json({ error: 'Someone else saved first – retrying' });
      return res.status(200).json({ v: nv });
    }
    return res.status(405).json({ error: 'Method not allowed' });
  } catch (e) { return res.status(500).json({ error: String(e.message || e) }); }
};
