from vision_models import analyze_image


def process_image_async(image_id, path):
    analyze_image.delay(image_id, path)
