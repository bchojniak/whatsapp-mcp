import os
import subprocess
import tempfile


def convert_to_opus_ogg(input_file, output_file=None, bitrate="32k", sample_rate=24000):
    """
    Convert an audio file to Opus format in an Ogg container.

    Args:
        input_file (str): Path to the input audio file
        output_file (str, optional): Path to save the output file. If None, replaces the
                                    extension of input_file with .ogg
        bitrate (str, optional): Target bitrate for Opus encoding (default: "32k")
        sample_rate (int, optional): Sample rate for output (default: 24000)

    Returns:
        str: Path to the converted file

    Raises:
        FileNotFoundError: If the input file doesn't exist
        RuntimeError: If the ffmpeg conversion fails
    """
    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    if output_file is None:
        output_file = os.path.splitext(input_file)[0] + ".ogg"

    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    cmd = [
        "ffmpeg",
        "-i",
        input_file,
        "-c:a",
        "libopus",
        "-b:a",
        bitrate,
        "-ar",
        str(sample_rate),
        "-application",
        "voip",  # Optimize for voice
        "-vbr",
        "on",  # Variable bitrate
        "-compression_level",
        "10",  # Maximum compression
        "-frame_duration",
        "60",  # 60ms frames (good for voice)
        "-y",  # Overwrite output file if it exists
        output_file,
    ]

    try:
        subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        return output_file
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Failed to convert audio. You likely need to install ffmpeg {e.stderr}"
        )


def convert_to_opus_ogg_temp(input_file, bitrate="32k", sample_rate=24000):
    """
    Convert an audio file to Opus format in an Ogg container and store in a temporary file.

    Args:
        input_file (str): Path to the input audio file
        bitrate (str, optional): Target bitrate for Opus encoding (default: "32k")
        sample_rate (int, optional): Sample rate for output (default: 24000)

    Returns:
        str: Path to the temporary file with the converted audio

    Raises:
        FileNotFoundError: If the input file doesn't exist
        RuntimeError: If the ffmpeg conversion fails
    """
    with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as temp_file:
        output_file = temp_file.name

    try:
        convert_to_opus_ogg(input_file, output_file, bitrate, sample_rate)
        return output_file
    except Exception:
        if os.path.exists(output_file):
            os.unlink(output_file)
        raise


if __name__ == "__main__":
    # Example usage
    import sys

    if len(sys.argv) < 2:
        print("Usage: python audio.py input_file [output_file]")
        sys.exit(1)

    input_file = sys.argv[1]

    try:
        result = convert_to_opus_ogg_temp(input_file)
        print(f"Successfully converted to: {result}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
