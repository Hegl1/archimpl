from cmath import exp
import pytest
from mosaic import loader

@pytest.mark.parametrize(
    "test_input,expected",[
        (loader.load_data("data/kemper/studenten.table"),"[('24002', 'Xenokrates', '18'), ('25403', 'Jonas', '12'), ('26120', 'Fichte', '10'), ('26830', 'Aristoxenos', '8'), ('27550', 'Schopenhauer', '6'), ('28106', 'Carnap', '3'), ('29120', 'Theophrastos', '2'), ('29555', 'Feuerbach', '2')]"),
        (loader.load_data("data/kemper/assistenten.table"),"[('3002', 'Platon', 'Ideenlehre', '2125'), ('3003', 'Aristoteles', 'Syllogistik', '2125'), ('3004', 'Wittgenstein', 'Sprachtheorie', '2126'), ('3005', 'Rhetikus', 'Planetenbewegung', '2127'), ('3006', 'Newton', 'Keplersche Gesetze', '2127'), ('3007', 'Spinoza', 'Gott und Natur', '2134')]"),
        (loader.load_data("data/kemper/professoren.table"),"[('2125', 'Sokrates', 'C4', '226'), ('2126', 'Russel', 'C4', '232'), ('2127', 'Kopernikus', 'C3', '310'), ('2133', 'Popper', 'C3', '52'), ('2134', 'Augustinus', 'C3', '309'), ('2136', 'Curie', 'C4', '36'), ('2137', 'Kant', 'C4', '7')]")])
def test_eval(test_input,expected):
    assert str(test_input) == expected
