defmodule DBNTest do
  use ExUnit.Case
  doctest DBN

  test "greets the world" do
    assert DBN.hello() == :world
  end
end
