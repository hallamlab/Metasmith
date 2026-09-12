// A recipe's fingerprint, for one question: is the plan on the page the one
// this recipe produces?
//
// Not a cryptographic digest, deliberately. It is minted here, handed to the
// solve, stored beside the plan the solve wrote, and compared here against a
// value minted the same way -- nothing ever verifies it or trusts it as a claim
// about content. What it buys over comparing the recipes themselves is size: a
// result carries eleven characters instead of a second copy of the request.
//
// Synchronous, which `crypto.subtle` is not, and that decides the shape of
// every caller: the comparison is a `$derived` off the recipe rather than an
// effect chasing a promise and writing the answer into state a render behind.
//
// The one property it has to have is stability. A stored fingerprint outlives
// the page that wrote it, so changing this function -- or the shape of what is
// handed to it -- retires every fingerprint on disk and every solved workflow
// reads as changed until it is solved again. That is the safe direction to fail
// in, and it is still not free.

/**
 * A stable short digest of any JSON-serialisable value.
 *
 * cyrb53: two 32-bit lanes mixed independently and combined into the 53 bits a
 * JS number carries exactly. `JSON.stringify` is the serialiser, so **key order
 * is part of the input** -- callers hand in a canonical form (see
 * `normalizeRows`, `targetsToWire`) rather than whatever object they happen to
 * be holding.
 */
export function fingerprint(value) {
  const s = JSON.stringify(value)
  let h1 = 0xdeadbeef
  let h2 = 0x41c6ce57
  for (let i = 0; i < s.length; i++) {
    const ch = s.charCodeAt(i)
    h1 = Math.imul(h1 ^ ch, 2654435761)
    h2 = Math.imul(h2 ^ ch, 1597334677)
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909)
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909)
  return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(36)
}
