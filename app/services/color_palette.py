"""
Service for extracting color palettes from images.

Uses ColorThief library to extract dominant colors from team logos.
"""

import io
from colorthief import ColorThief
from PIL import Image


class ColorPaletteService:
    """Extract color palettes from images."""

    @staticmethod
    def rgb_to_hex(rgb: tuple[int, int, int]) -> str:
        """Convert RGB tuple to hex color code."""
        return f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"

    @staticmethod
    def extract_palette(image_bytes: bytes, color_count: int = 3) -> list[str]:
        """
        Extract dominant colors from an image.

        Args:
            image_bytes: Image content as bytes
            color_count: Number of colors to extract (default: 3)

        Returns:
            List of hex color codes, e.g., ['#FF5733', '#33FF57', '#3357FF']
        """
        try:
            # Convert to RGB if needed (some logos are PNG with transparency)
            img = Image.open(io.BytesIO(image_bytes))

            # Convert RGBA to RGB on white background
            if img.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                img = background

            # Save to bytes for ColorThief
            img_bytes = io.BytesIO()
            img.save(img_bytes, format='PNG')
            img_bytes.seek(0)

            # Extract colors
            color_thief = ColorThief(img_bytes)

            if color_count == 1:
                dominant_color = color_thief.get_color(quality=1)
                return [ColorPaletteService.rgb_to_hex(dominant_color)]
            else:
                palette = color_thief.get_palette(color_count=color_count, quality=1)
                return [ColorPaletteService.rgb_to_hex(rgb) for rgb in palette]

        except Exception as e:
            raise RuntimeError(f"Failed to extract colors: {e}")

    @staticmethod
    def extract_team_colors(image_bytes: bytes) -> dict[str, str | None]:
        """
        Extract primary, secondary, and accent colors for a team.

        Args:
            image_bytes: Team logo image bytes

        Returns:
            Dict with keys: primary_color, secondary_color, accent_color
        """
        try:
            colors = ColorPaletteService.extract_palette(image_bytes, color_count=3)

            return {
                "primary_color": colors[0] if len(colors) > 0 else None,
                "secondary_color": colors[1] if len(colors) > 1 else None,
                "accent_color": colors[2] if len(colors) > 2 else None,
            }
        except Exception as e:
            # Return None values if extraction fails
            return {
                "primary_color": None,
                "secondary_color": None,
                "accent_color": None,
            }
