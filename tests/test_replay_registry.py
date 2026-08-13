from scgrep.replay_registry import ReplayRegistry


def test_counts_and_first_arrival():
    reg = ReplayRegistry()
    channel = "replay/a/wis2/ca-eccc-msc-global-replay/uuid/monitor/a/wis2/ca-eccc-msc"
    counter = reg.register(channel)
    reg.handle_replay(channel)
    reg.handle_replay(channel)
    count, first = counter.snapshot()
    assert count == 2
    assert first is not None


def test_subtopic_beneath_channel_matches():
    reg = ReplayRegistry()
    channel = "replay/a/wis2/ca-eccc-msc-global-replay/uuid/cache/a/wis2/x"
    counter = reg.register(channel)
    reg.handle_replay(channel + "/deeper/leaf")
    assert counter.snapshot()[0] == 1


def test_unrelated_topic_ignored():
    reg = ReplayRegistry()
    channel = "replay/a/wis2/ca-eccc-msc-global-replay/uuid/monitor/a/wis2/ca-eccc-msc"
    counter = reg.register(channel)
    reg.handle_replay("replay/a/wis2/other-centre/uuid/monitor/a/wis2/ca-eccc-msc")
    assert counter.snapshot()[0] == 0


def test_unregister_stops_counting():
    reg = ReplayRegistry()
    channel = "replay/a/wis2/c/uuid/t"
    counter = reg.register(channel)
    reg.unregister(channel)
    reg.handle_replay(channel)
    assert counter.snapshot()[0] == 0
