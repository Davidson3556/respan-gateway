"""Images and embeddings through the same client.

    OPENAI_API_KEY=sk-... python examples/media.py
"""

import base64

import respan_gateway as rg


def main(gw: rg.Gateway) -> None:
    client = gw.openai()
    image = client.images.generate(model="gpt-image-1", prompt="a red fox", size="1024x1024")
    png = base64.b64decode(image.data[0].b64_json)
    print(f"image: {len(png)} bytes")
    embedding = client.embeddings.create(model="text-embedding-3-small", input="hello")
    print(f"embedding: {len(embedding.data[0].embedding)} numbers")
    for model in gw.models(kind="image"):  # every provider's models, with their kind
        print(model["id"], "by", model["owned_by"])


if __name__ == "__main__":
    with rg.Gateway() as gw:
        main(gw)
