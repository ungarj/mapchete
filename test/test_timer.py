from mapchete import Timer


def test_timer():
    timer1 = Timer(elapsed=1000)
    timer2 = Timer(elapsed=2000)
    timer3 = Timer(elapsed=1000)

    assert timer1 < timer2
    assert timer1 <= timer2
    assert timer2 > timer3
    assert timer2 >= timer3
    assert timer1 == timer3
    assert timer1 != timer2
    assert timer1 + timer3 == timer2
    assert timer2 - timer3 == timer1

    timer = Timer(elapsed=60)
    assert str(timer) == "1m 0s"

    timer = Timer(elapsed=60)
    assert str(timer) == "1m 0s"
    timer = Timer(elapsed=3700)
    assert str(timer) == "1h 1m 40s"

    assert "Timer" in timer.__repr__()
