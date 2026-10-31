import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex } from "@noble/hashes/utils.js";
import { verifyMessage } from "viem";
import { createIdentityFromPrivateKey } from "./identity";
import { signedFetch } from "./signer";
import type { AuthIdentity } from "./types";

describe("when signing a plain JSON schedule mutation", () => {
  let identity: AuthIdentity;
  let body: string;
  let requests: RequestInit[];
  beforeEach(async () => {
    identity = await createIdentityFromPrivateKey(`0x${"01".repeat(32)}`);
    body = JSON.stringify({ name: "Signed calendar" });
    requests = [];
    vi.stubGlobal("fetch", vi.fn(async (_url: string, init: RequestInit) => {
      requests.push(init);
      return new Response("{}", { status: 200 });
    }));
  });
  afterEach(() => vi.unstubAllGlobals());
  it("should bind the exact body digest and unique nonce into the valid signature", async () => {
    await signedFetch(identity, "https://node.test/events/api/schedules", { method: "POST", body });
    await signedFetch(identity, "https://node.test/events/api/schedules", { method: "POST", body });
    expect(requests.map((request) => request.body)).toEqual([body, body]);
    expect(JSON.parse(new Headers(requests[0].headers).get("x-identity-metadata")!)).toMatchObject({ body_sha256: bytesToHex(sha256(new TextEncoder().encode(body))), nonce: expect.stringMatching(/^[\da-f-]{36}$/) });
    expect(new Headers(requests[0].headers).get("x-identity-metadata")).not.toEqual(new Headers(requests[1].headers).get("x-identity-metadata"));
    expect(JSON.parse(new Headers(requests[0].headers).get("x-identity-auth-chain-2")!).payload).toContain(new Headers(requests[0].headers).get("x-identity-metadata"));
    await expect(verifyMessage({ address: identity.ephemeral.address as `0x${string}`, message: JSON.parse(new Headers(requests[0].headers).get("x-identity-auth-chain-2")!).payload, signature: JSON.parse(new Headers(requests[0].headers).get("x-identity-auth-chain-2")!).signature })).resolves.toBe(true);
  });
  it("should bind patches signed using the route-local path too", async () => {
    await signedFetch(identity, "https://node.test/events/api/schedules/id", { method: "PATCH", signPath: "/api/schedules/id", body });
    expect(JSON.parse(new Headers(requests[0].headers).get("x-identity-metadata")!)).toHaveProperty("body_sha256");
  });
  it("should refuse a schedule body that cannot be bound as serialized JSON", async () => {
    await expect(signedFetch(identity, "https://node.test/events/api/schedules", { method: "POST", body: new Blob([body]) })).rejects.toThrow("serialized JSON");
    expect(requests).toEqual([]);
  });
});
