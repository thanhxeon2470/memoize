defmodule Memoize.CacheStrategy.Default.Persistent do
  @moduledoc false

  @behaviour Memoize.CacheStrategy

  def init(opts) do
    # Default global settings
    #
    # config :memoize, Memoize.CacheStrategy.Default,
    #   expires_in: 1000
    expires_in =
      Application.get_env(:memoize, Memoize.CacheStrategy.Default, []) |> Keyword.get(:expires_in, :infinity)

    opts = Keyword.put(opts, :expires_in, expires_in)
    opts
  end

  def tab(_key) do
    __MODULE__
  end

  def cache(_key, _value, opts) do
    expires_in = Keyword.get(opts, :expires_in, Memoize.Config.opts().expires_in)

    expired_at =
      case expires_in do
        :infinity -> :infinity
        value -> System.monotonic_time(:millisecond) + value
      end

    expired_at
  end

  def read(key, _value, expired_at) do
    if expired_at != :infinity && System.monotonic_time(:millisecond) > expired_at do
      invalidate(key)
      :retry
    else
      :ok
    end
  end

  def invalidate() do
    try do
      :persistent_term.get()
      |> Enum.each(fn {key, value} ->
        value
        |> case do
          {_, {:completed, _, _}} ->
            :persistent_term.erase(key)

          _ ->
            :ok
        end
      end)

      1
    rescue
      _ ->
        0
    end
  end

  def invalidate(key) do
    if :persistent_term.erase(key) do
      1
    else
      0
    end
  end

  def garbage_collect() do
    1
  end

  def persistent() do
    __MODULE__
  end
end
