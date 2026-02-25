# app/utils/image_probe.py


from PIL import Image

def probe_image(path: str) -> dict:
    with Image.open(path) as im:
        im.load()
        return {
            "width": im.width,
            "height": im.height,
            "format": im.format,
            "mode": im.mode,
        }


