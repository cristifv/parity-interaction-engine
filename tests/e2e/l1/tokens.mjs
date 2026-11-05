// IE step 6 e2e: the agents' dev tokens and the gateway's public JWKS from the local-dev key
// (secrets/local-dev/gateway-dev-jwt.jwk.json; read, never printed).
//   node tokens.mjs <private jwk> <out jwks.json> <tokens dir> <agent>...
// Each token (acme/<agent>, roles [agent], 1 h) is written to <tokens dir>/token-<a> with mode 600,
// where <a> is the agent id's last part (agent-a1 -> a1).
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
const [jwkPath, jwksOut, dir, ...agents] = process.argv.slice(2);
const jwk = JSON.parse(fs.readFileSync(jwkPath, "utf8"));
const priv = crypto.createPrivateKey({ key: jwk, format: "jwk" });
const pub = crypto.createPublicKey(priv).export({ format: "jwk" });
if (jwk.kid) pub.kid = jwk.kid;
pub.use = "sig";
pub.alg = "ES256";
fs.writeFileSync(jwksOut, JSON.stringify({ keys: [pub] }));
const b64 = (b) => Buffer.from(b).toString("base64url");
for (const agent of agents) {
  const head = { alg: "ES256", typ: "JWT", ...(jwk.kid ? { kid: jwk.kid } : {}) };
  const claims = { iss: "parity-gateway-dev", sub: agent, tenant_id: "acme", agent_id: agent, roles: ["agent"],
    sid: crypto.randomUUID(), exp: Math.floor(Date.now() / 1000) + 3600 };
  const data = `${b64(JSON.stringify(head))}.${b64(JSON.stringify(claims))}`;
  const sig = crypto.sign("sha256", Buffer.from(data), { key: priv, dsaEncoding: "ieee-p1363" });
  const file = path.join(dir, `token-${agent.replace(/^agent-/, "")}`);
  fs.writeFileSync(file, `${data}.${b64(sig)}`, { mode: 0o600 });
}
