use std::path::Path;
use std::process::Command;

pub(crate) fn ffmpeg_available() -> bool {
    Command::new("ffmpeg")
        .arg("-version")
        .output()
        .is_ok_and(|output| output.status.success())
}

pub(crate) fn generate_test_video(path: &Path) -> bool {
    Command::new("ffmpeg")
        .args([
            "-y",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=duration=4:size=320x240:rate=25",
            "-g",
            "12",
            "-c:v",
            "mpeg4",
        ])
        .arg(path)
        .output()
        .is_ok_and(|output| output.status.success())
}
