owrite_tracker = function(){
    tracker_path = glue::glue('data/{n}/{d}/data_tracker.json',
        n=network, d=domain)
    jsonlite::write_json(held_data, tracker_path)
}
