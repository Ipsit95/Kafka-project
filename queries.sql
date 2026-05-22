INSERT_WIKIPEDIA = """
INSERT INTO sample_first_schema.wikipedia_creats
    (event_id, wiki, title, editor, edit_type, is_bot,
     bytes_delta, comment, page_url, edit_ts)
VALUES
    (%(event_id)s, %(wiki)s, %(title)s, %(editor)s, %(edit_type)s,
     %(is_bot)s, %(bytes_delta)s, %(comment)s, %(page_url)s, %(timestamp)s)
ON CONFLICT (event_id) DO NOTHING;
"""
