import pytest

from cluo.core import Batch, Record
from cluo.stages import SpecialCharacterCleanerStage
from tests.conftest import parametrize_processes


@parametrize_processes(SpecialCharacterCleanerStage)
@pytest.mark.parametrize(
    "input_,output",
    [
        ("kožušček", "kozuscek"),
        ("aa«aàç±ç²£çñññ¤¿", "aa<<aac+-c2PScnnn$??"),
    ],
)
def test_whitespace_is_trimmed(processes, input_, output):
    stage = SpecialCharacterCleanerStage(fields={"string"}, processes=processes)
    input_ = Batch(records=[Record(data={"string": input_})])
    result = stage(input_)
    assert result.records[0].data["string"] == output
