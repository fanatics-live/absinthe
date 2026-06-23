defmodule Absinthe.Phase.Document.Validation.UniqueFragmentNamesTest do
  @phase Absinthe.Phase.Document.Validation.UniqueFragmentNames

  use Absinthe.ValidationPhaseCase,
    phase: @phase,
    async: true

  alias Absinthe.{Blueprint, Phase, Pipeline}

  defp duplicate_fragment(name, line) do
    bad_value(
      Blueprint.Document.Fragment.Named,
      @phase.error_message(name),
      line,
      name: name
    )
  end

  describe "Validate: Unique fragment names" do
    test "no fragments" do
      assert_passes_validation(
        """
        {
          dog { name }
        }
        """,
        []
      )
    end

    test "one fragment" do
      assert_passes_validation(
        """
        {
          dog { ...dogFields }
        }
        fragment dogFields on Dog {
          name
        }
        """,
        []
      )
    end

    test "multiple unique fragments" do
      assert_passes_validation(
        """
        fragment fragA on Dog {
          name
        }
        fragment fragB on Dog {
          nickname
        }
        """,
        []
      )
    end

    test "duplicate fragment names" do
      assert_fails_validation(
        """
        fragment fragA on Dog {
          name
        }
        fragment fragA on Dog {
          nickname
        }
        """,
        [],
        [
          duplicate_fragment("fragA", 1),
          duplicate_fragment("fragA", 4)
        ]
      )
    end

    test "many duplicate fragment names" do
      assert_fails_validation(
        """
        fragment fragA on Dog {
          name
        }
        fragment fragA on Dog {
          nickname
        }
        fragment fragA on Dog {
          barkVolume
        }
        """,
        [],
        [
          duplicate_fragment("fragA", 1),
          duplicate_fragment("fragA", 4),
          duplicate_fragment("fragA", 7)
        ]
      )
    end

    test "duplicate and unique fragments mixed" do
      assert_fails_validation(
        """
        fragment fragA on Dog {
          name
        }
        fragment fragB on Dog {
          nickname
        }
        fragment fragA on Dog {
          barkVolume
        }
        """,
        [],
        [
          duplicate_fragment("fragA", 1),
          duplicate_fragment("fragA", 7)
        ]
      )
    end
  end

  # Regression test for CSV-TK
  # UniqueFragmentNames.duplicate?/2 called Enum.count/2 (a full linear scan)
  # for every fragment, producing O(N²) comparisons per document. An attacker
  # can stall a worker for seconds with a single request containing many fragments.
  describe "security: algorithmic complexity" do
    test "validation of many unique fragments completes in linear time" do
      n = 5_000

      fragments = Enum.map_join(1..n, " ", fn i -> "fragment f#{i} on Dog { name }" end)
      document = "{ dog { name } } " <> fragments
      {:ok, blueprint, _} = Pipeline.run(document, [Phase.Parse, Phase.Blueprint])
      {elapsed_us, _result} = :timer.tc(fn -> @phase.run(blueprint, []) end)
      elapsed_ms = div(elapsed_us, 1_000)

      assert elapsed_ms < 50,
             "UniqueFragmentNames took #{elapsed_ms}ms for #{n} unique fragments — " <>
               "expected < 50ms (linear). Quadratic behaviour detected."
    end
  end
end
