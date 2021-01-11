from corva.models.stream import StreamEvent


class FilterStreamEvent:
    @classmethod
    def run(
         cls,
         event: StreamEvent,
         by_timestamp: bool,
         by_depth: bool,
         last_processed_timestamp: int,
         last_processed_depth: float
    ) -> StreamEvent:
        records = event.records

        if event.is_completed:
            records = records[:-1]  # remove "completed" record

        new_records = []
        for record in records:
            if by_timestamp and record.timestamp <= last_processed_timestamp:
                continue
            if by_depth and record.measured_depth <= last_processed_depth:
                continue

            new_records.append(record)

        return event.copy(update={'records': new_records}, deep=True)
