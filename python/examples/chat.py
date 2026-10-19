"""Chat through the OpenAI SDK, in-process: no socket, no server.

    OPENAI_API_KEY=sk-... python examples/chat.py
"""

import respan_gateway as rg


def main(gw: rg.Gateway, model: str = "gpt-6-luna") -> str:
    client = gw.openai()
    reply = client.chat.completions.create(model=model, messages=[{"role": "user", "content": "Say hi."}])
    text = reply.choices[0].message.content or ""
    print(text)
    # A streamed reply arrives chunk by chunk.
    stream = client.chat.completions.create(
        model=model, messages=[{"role": "user", "content": "Say hi."}], stream=True
    )
    for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)
    print()
    return text


if __name__ == "__main__":
    with rg.Gateway() as gw:  # the vendors the environment has keys for
        main(gw)
