# app/utils/image_probe.py


from PIL import Image

Image.MAX_IMAGE_PIXELS = None  # полностью отключить защиту
# ======== NEW ========
def probe_image(path: str) -> dict:
    with Image.open(path) as im:
        # im.load()
        # ======== NEW ========
        return {
            "width": im.width,
            "height": im.height,
            "format": im.format,
            "mode": im.mode,
        }


