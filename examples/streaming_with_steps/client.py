import asyncio

from rfnry_thread import Text


async def helper_answers(helper, thread_id, question):
    await helper.working(thread_id, label="Reading rules/refunds.md")
    await helper.step(
        thread_id,
        name="Read",
        input={"path": "rules/refunds.md"},
        output=[Text(text="A double charge is refunded in full.")],
        duration_ms=12,
        in_reply_to=question.id,
    )
    answer = "The second charge is refunded in full, per the refunds rule."
    async with helper.stream(thread_id, in_reply_to=question.id) as reply:
        for word in answer.split(" "):
            await reply.write(word + " ")
            await asyncio.sleep(0.05)
        await reply.finish(reply.text.strip())
    await helper.idle(thread_id)
