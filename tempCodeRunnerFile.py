target_tuples = set(map(tuple, target_df.to_records(index=False)))
            source_tuples = set(map(tuple, source_df.to_records(index=False)))