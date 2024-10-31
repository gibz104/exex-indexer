/// Sanitize a byte slice to ensure it's valid UTF-8.
pub(crate) fn sanitize_bytes(input: &[u8]) -> String {
    // First, remove all null bytes
    let without_nulls: Vec<u8> = input.iter().filter(|&&b| b != 0).cloned().collect();

    // Then, convert to a string, replacing any invalid UTF-8 sequences
    String::from_utf8_lossy(&without_nulls).into_owned()
}