from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_serializer

from bamaq.domain import Status


class CreateTransaction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    customer_id: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)
    ]
    value: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]


class TransactionView(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    customer_id: str
    value: Decimal = Field(
        description="Valor em BRL, serializado como string com duas casas decimais"
    )
    status: Status

    @field_serializer("value")
    def serialize_value(self, value: Decimal) -> str:
        return format(value, ".2f")
