defmodule DBNTest do
  use ExUnit.Case

  doctest DBN

  @data_path Path.expand("./support/databento/dbn/tests/data", __DIR__)

  describe "DBN on .dbn" do
    @dbn_files File.ls!(@data_path) |> Enum.filter(&String.ends_with?(&1, ".dbn"))

    for file <- @dbn_files do
      test "decodes #{file}" do
        data = Path.join(@data_path, unquote(file)) |> File.read!()
        assert %DBN.Metadata{} = DBN.decode(data)
        # TODO: Mappings
        # TODO: Records
      end
    end
  end

  describe "DBN on .dbn.zst" do
    @dbn_zst_files File.ls!(@data_path) |> Enum.filter(&String.ends_with?(&1, ".dbn.zst"))

    for file <- @dbn_zst_files do
      test "decodes #{file}" do
        data = Path.join(@data_path, unquote(file)) |> File.read!()
        assert true
        # TODO: Metadata
        # TODO: Mappings
        # TODO: Records
      end
    end
  end

  # TODO: handle `.dbz`, `.dbn.frag`, `.dbn.frag.zst`
end
