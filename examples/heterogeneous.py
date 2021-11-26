from typing import List
from functools import partial

import psycopg2
from pydantic import BaseModel
from wunderkafka import ConsumerConfig

from blocks import App, processor
from blocks.kafka import Batch, InputTopic, KafkaSource
from blocks.postgres import Query, Table, PostgresWriter


class Mail(BaseModel):
    receivers: str
    sender: str


class MailBatch(Batch):
    ...


class MailBatchTable(Table):
    rows = List[Mail]


@processor
def batch_printer(event: MailBatch) -> None:
    print('MailBatch: {0}'.format(len(event.events)))


@processor
def batch2table(event: MailBatch) -> MailBatchTable:
    return MailBatchTable(rows=event.events)


if __name__ == '__main__':
    topics = [InputTopic('mails', Mail, from_beginning=True, max_empty_polls=100, batch_event=MailBatch)]
    source = KafkaSource(topics=topics, config=ConsumerConfig(...))

    inserts = [Query('insert into mytable ({}) values ({})', MailBatchTable)]
    destionation = PostgresWriter(quieries=inserts, connection_factory=partial(psycopg2.connect, ...))

    blocks = (source, batch_printer(), batch2table(), destionation)

    App(blocks).run(once=True)
