defmodule Absinthe.Phase.Document.Validation.ExecutableDefinitionsTest do
  @phase Absinthe.Phase.Document.Validation.ExecutableDefinitions

  use Absinthe.PhaseCase, phase: @phase, schema: __MODULE__.Schema, async: true

  defmodule Schema do
    use Absinthe.Schema

    query do
      field :hello, :string do
        resolve fn _, _, _ -> {:ok, "world"} end
      end
    end
  end

  describe "documents containing only executable definitions" do
    test "passes for a single anonymous operation" do
      assert {:ok, %{execution: %{validation_errors: []}}, _} = run_phase("{ hello }", [])
    end

    test "passes for a named query" do
      assert {:ok, %{execution: %{validation_errors: []}}, _} =
               run_phase("query hello { hello }", [])
    end

    test "passes for a mutation" do
      assert {:ok, %{execution: %{validation_errors: []}}, _} =
               run_phase("mutation hello { hello }", [])
    end

    test "passes for a subscription" do
      assert {:ok, %{execution: %{validation_errors: []}}, _} =
               run_phase("subscription hello { hello }", [])
    end

    test "passes for an operation plus a fragment" do
      query = """
      fragment Bar on Query { hello }
      query Foo { ...Bar }
      """

      assert {:ok, %{execution: %{validation_errors: []}}, _} = run_phase(query, [])
    end
  end

  describe "documents containing type system definitions are rejected" do
    test "rejects a directive definition" do
      document = """
      directive @skip on FIELD
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: [error]}}, _} = run_phase(document, [])
      assert %Absinthe.Phase.Error{phase: @phase, message: message} = error
      assert "Directive `@skip` is not an executable definition" = message
    end

    test "rejects an object type definition" do
      document = """
      type Foo { field: String }
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: [error]}}, _} = run_phase(document, [])
      assert %Absinthe.Phase.Error{phase: @phase, message: message} = error
      assert "Type `Foo` is not an executable definition" = message
    end

    test "rejects an enum type definition" do
      document = """
      enum Color { RED GREEN BLUE }
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: [error]}}, _} = run_phase(document, [])
      assert %Absinthe.Phase.Error{phase: @phase, message: message} = error
      assert "Enum `Color` is not an executable definition" = message
    end

    test "rejects an input object type definition" do
      document = """
      input FooInput { field: String }
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: [error]}}, _} = run_phase(document, [])
      assert %Absinthe.Phase.Error{phase: @phase, message: message} = error
      assert "Input object `FooInput` is not an executable definition" = message
    end

    test "rejects an interface type definition" do
      document = """
      interface Node { id: ID! }
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: [error]}}, _} = run_phase(document, [])
      assert %Absinthe.Phase.Error{phase: @phase, message: message} = error
      assert "Interface `Node` is not an executable definition" = message
    end

    test "rejects a scalar type definition" do
      document = """
      scalar DateTime
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: [error]}}, _} = run_phase(document, [])
      assert %Absinthe.Phase.Error{phase: @phase, message: message} = error
      assert "Scalar `DateTime` is not an executable definition" = message
    end

    test "rejects a union type definition" do
      document = """
      union Result = Foo | Bar
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: [error]}}, _} = run_phase(document, [])
      assert %Absinthe.Phase.Error{phase: @phase, message: message} = error
      assert "Union `Result` is not an executable definition" = message
    end

    test "rejects a schema definition" do
      document = """
      schema { query: Query }
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: [error]}}, _} = run_phase(document, [])
      assert %Absinthe.Phase.Error{phase: @phase, message: message} = error
      assert "A schema definition is not an executable definition" = message
    end

    test "rejects a type extension" do
      document = """
      extend type Query { extra: String }
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: [error]}}, _} = run_phase(document, [])
      assert %Absinthe.Phase.Error{phase: @phase, message: message} = error
      assert "An extension of `Query` is not an executable definition" = message
    end

    test "produces one error per offending definition" do
      document = """
      directive @one on FIELD
      type Foo { field: String }
      scalar DateTime
      query { hello }
      """

      assert {:error, %{execution: %{validation_errors: errors}}, _} = run_phase(document, [])
      assert Enum.all?(errors, &match?(%Absinthe.Phase.Error{phase: @phase}, &1))
      messages = Enum.map(errors, & &1.message)
      assert "Directive `@one` is not an executable definition" = Enum.at(messages, 0)
      assert "Type `Foo` is not an executable definition" = Enum.at(messages, 1)
      assert "Scalar `DateTime` is not an executable definition" = Enum.at(messages, 2)
    end
  end
end
