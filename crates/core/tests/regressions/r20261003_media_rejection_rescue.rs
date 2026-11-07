//! Defect: a provider media-size rejection (one oversized image, too many
//! images, an over-page PDF) reached a turn as a plain 400 and failed it on the
//! spot, even though the reactive-compaction rescue — whose summary request
//! strips images — is exactly the recovery that sheds the offending payload.
//! Root cause: the classifier had no media-size predicate, so the turn loop
//! could not route those 400s onto the 413 rescue path.
//! Fixed by: provider::classify::is_media_size_error plus the turn loop's
//! media branch (rescue once, surface the provider wording when it fails).
//! The patterns are intentionally conservative; a wording drift
//! degrades to the old fail-fast behavior instead of mis-rescuing.

use klaus_agent_core::provider::classify::is_media_size_error;

#[test]
fn oversized_image_wording_is_a_media_rejection() {
    assert!(is_media_size_error(
        "messages.1.content.1.image: image exceeds the maximum allowed size of 5 MB"
    ));
    assert!(is_media_size_error(
        "Image exceeds maximum dimensions of 8000x8000"
    ));
}

#[test]
fn many_image_and_pdf_wording_are_media_rejections() {
    assert!(is_media_size_error(
        "image dimensions exceed the supported limit for a many-image request"
    ));
    assert!(is_media_size_error(
        "document exceeds the maximum of 100 PDF pages"
    ));
}

#[test]
fn unrelated_rejections_are_not_media_rejections() {
    // A whole-prompt overflow stays on the 413 path…
    assert!(!is_media_size_error(
        "prompt is too long: 300000 tokens > 200000 maximum"
    ));
    // …and so does an ordinary invalid request that merely mentions a size.
    assert!(!is_media_size_error(
        "request body size exceeds the 32 MB limit"
    ));
    assert!(!is_media_size_error("the document has 10 PDF pages"));
}
