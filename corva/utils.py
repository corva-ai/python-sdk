from corva.models.stream import StreamEvent, StreamEventData


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
        data = []
        for subdata in event:  # type: StreamEventData
            data.append(
                cls._filter_event_data(
                    data=subdata,
                    by_timestamp=by_timestamp,
                    by_depth=by_depth,
                    last_processed_timestamp=last_processed_timestamp,
                    last_processed_depth=last_processed_depth
                )
            )

        return StreamEvent(data)

    @staticmethod
    def _filter_event_data(
         data: StreamEventData,
         by_timestamp: bool,
         by_depth: bool,
         last_processed_timestamp: int,
         last_processed_depth: float
    ) -> StreamEventData:
        records = data.records

        if data.is_completed:
            records = records[:-1]  # remove "completed" record

        new_records = []
        for record in records:
            if by_timestamp and record.timestamp <= last_processed_timestamp:
                continue
            if by_depth and record.measured_depth <= last_processed_depth:
                continue

            new_records.append(record)

        return data.copy(update={'records': new_records}, deep=True)
