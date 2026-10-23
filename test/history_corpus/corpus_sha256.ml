(** Minimal FIPS 180-4 SHA-256 for the corpus manifest checksums.

    The OCaml standard library provides MD5 and BLAKE2 but not SHA-256, and the
    corpus manifest records SHA-256 so maintainers can check it with standard
    tools ([shasum -a 256], [sha256sum]). Adding a hashing dependency only for a
    test would widen the audited dependency set, so this test-only helper
    implements the algorithm directly. It operates on native [int] values
    masked to 32 bits (OCaml 5 is 64-bit only on supported targets) and is
    checked against published test vectors before any manifest is trusted. *)

(** Round constants: the first 32 bits of the fractional parts of the cube
    roots of the first 64 primes. *)
let k =
  [|
    0x428a2f98; 0x71374491; 0xb5c0fbcf; 0xe9b5dba5; 0x3956c25b; 0x59f111f1;
    0x923f82a4; 0xab1c5ed5; 0xd807aa98; 0x12835b01; 0x243185be; 0x550c7dc3;
    0x72be5d74; 0x80deb1fe; 0x9bdc06a7; 0xc19bf174; 0xe49b69c1; 0xefbe4786;
    0x0fc19dc6; 0x240ca1cc; 0x2de92c6f; 0x4a7484aa; 0x5cb0a9dc; 0x76f988da;
    0x983e5152; 0xa831c66d; 0xb00327c8; 0xbf597fc7; 0xc6e00bf3; 0xd5a79147;
    0x06ca6351; 0x14292967; 0x27b70a85; 0x2e1b2138; 0x4d2c6dfc; 0x53380d13;
    0x650a7354; 0x766a0abb; 0x81c2c92e; 0x92722c85; 0xa2bfe8a1; 0xa81a664b;
    0xc24b8b70; 0xc76c51a3; 0xd192e819; 0xd6990624; 0xf40e3585; 0x106aa070;
    0x19a4c116; 0x1e376c08; 0x2748774c; 0x34b0bcb5; 0x391c0cb3; 0x4ed8aa4a;
    0x5b9cca4f; 0x682e6ff3; 0x748f82ee; 0x78a5636f; 0x84c87814; 0x8cc70208;
    0x90befffa; 0xa4506ceb; 0xbef9a3f7; 0xc67178f2;
  |]

(** Keeps the low 32 bits of an intermediate sum. *)
let mask = 0xffffffff

(** 32-bit right rotation. *)
let rotr x n = ((x lsr n) lor (x lsl (32 - n))) land mask

(** Returns the lowercase hexadecimal SHA-256 digest of [input]. *)
let digest_hex (input : string) =
  let length = String.length input in
  (* Padding: one 0x80 byte, zeros to 56 mod 64, then the 64-bit bit length. *)
  let padded_length = (length + 9 + 63) / 64 * 64 in
  let message = Bytes.make padded_length '\000' in
  Bytes.blit_string input 0 message 0 length;
  Bytes.set message length '\x80';
  let bits = length * 8 in
  for i = 0 to 7 do
    Bytes.set message
      (padded_length - 1 - i)
      (Char.chr ((bits lsr (8 * i)) land 0xff))
  done;
  let h =
    [|
      0x6a09e667; 0xbb67ae85; 0x3c6ef372; 0xa54ff53a; 0x510e527f; 0x9b05688c;
      0x1f83d9ab; 0x5be0cd19;
    |]
  in
  let w = Array.make 64 0 in
  for block = 0 to (padded_length / 64) - 1 do
    let base = block * 64 in
    for t = 0 to 15 do
      let byte i = Char.code (Bytes.get message (base + (4 * t) + i)) in
      w.(t) <- (byte 0 lsl 24) lor (byte 1 lsl 16) lor (byte 2 lsl 8) lor byte 3
    done;
    for t = 16 to 63 do
      let s0 =
        rotr w.(t - 15) 7 lxor rotr w.(t - 15) 18 lxor (w.(t - 15) lsr 3)
      in
      let s1 =
        rotr w.(t - 2) 17 lxor rotr w.(t - 2) 19 lxor (w.(t - 2) lsr 10)
      in
      w.(t) <- (w.(t - 16) + s0 + w.(t - 7) + s1) land mask
    done;
    let a = ref h.(0) and b = ref h.(1) and c = ref h.(2) and d = ref h.(3) in
    let e = ref h.(4) and f = ref h.(5) and g = ref h.(6) and hh = ref h.(7) in
    for t = 0 to 63 do
      let s1 = rotr !e 6 lxor rotr !e 11 lxor rotr !e 25 in
      let ch = !e land !f lxor (lnot !e land mask land !g) in
      let t1 = (!hh + s1 + ch + k.(t) + w.(t)) land mask in
      let s0 = rotr !a 2 lxor rotr !a 13 lxor rotr !a 22 in
      let maj = !a land !b lxor (!a land !c) lxor (!b land !c) in
      let t2 = (s0 + maj) land mask in
      hh := !g;
      g := !f;
      f := !e;
      e := (!d + t1) land mask;
      d := !c;
      c := !b;
      b := !a;
      a := (t1 + t2) land mask
    done;
    List.iteri
      (fun i value -> h.(i) <- (h.(i) + value) land mask)
      [ !a; !b; !c; !d; !e; !f; !g; !hh ]
  done;
  String.concat "" (Array.to_list (Array.map (Printf.sprintf "%08x") h))

(** Fails unless the implementation reproduces the FIPS 180-4 examples,
    including a two-block message. Called before any manifest check, so a
    broken hash cannot make every checksum comparison vacuous. *)
let self_test () =
  let check input expected =
    let actual = digest_hex input in
    if actual <> expected then
      failwith
        (Printf.sprintf "SHA-256 self-test failed for %S: %s" input actual)
  in
  check "" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  check "abc" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
  check "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
    "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
