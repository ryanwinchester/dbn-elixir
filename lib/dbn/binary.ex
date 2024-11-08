defmodule DBN.Binary do
  @moduledoc """
  Functions and macros to help with binary parsing and pattern-matchind.
  """

  # Macros to make binary pattern matching a bit cleaner and set up conventions
  # for DBN. For example, they use little-endian for the numbers.
  defmacro bits(n), do: quote(do: bitstring - size(unquote(n)))
  defmacro bytes(n), do: quote(do: binary - size(unquote(n)))
  defmacro uint(n), do: quote(do: little - unsigned - integer - size(unquote(n)))

  @doc """
  Trim null bytes from a binary.

  ## Examples

      iex> trim_nulls(<<1, 2, 3, 4, 0, 0, 0>>)
      <<1, 2, 3, 4>>

      iex> trim_nulls(<<0, 0, 0, 0>>)
      <<>>

      iex> trim_nulls(<<0, 0, 0, 1>>)
      <<0, 0, 0, 1>>

  """
  def trim_nulls(binary), do: trim_nulls(binary, byte_size(binary))

  defp trim_nulls(binary, size) do
    case binary do
      <<trimmed::bytes(size - 1), 0>> -> trim_nulls(trimmed, byte_size(trimmed))
      binary -> binary
    end
  end
end
