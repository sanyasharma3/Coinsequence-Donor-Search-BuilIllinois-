from typing import Any, Optional, Type
from uuid import UUID

import structlog
from app.api.api_v1.student.dto.search_profile import SearchUsersParams
from app.domain.student.data.profile import UserProfileProps
from app.domain.student.data.search_profile import PROFILE_QUERY_PARAMS_MAPPING
from app.domain.student.repository.db.profile import UserProfileRepository
from app.repository.db.schema.activity import Activity
from app.repository.db.schema.application import Application
from app.repository.db.schema.award import Award
from app.repository.db.schema.college_universities import CollegeUniversities
from app.repository.db.schema.course import Course
from app.repository.db.schema.education import Education
from app.repository.db.schema.grade import Grade
from app.repository.db.schema.profile import UserProfile
from app.repository.db.schema.roles import Roles
from app.repository.db.schema.voluntary import Voluntary
from app.repository.db.schema.work import Work
from app.shared.domain.data.page import Page, PageMetadata
from app.shared.repository.db.base import BaseDBRepository
from app.shared.utils.error import DomainError
from sqlalchemy import and_, desc, distinct, func, or_, update
from sqlalchemy.orm import contains_eager

logger = structlog.get_logger()


class UserProfileDBRepository(
    BaseDBRepository[UserProfileProps, UserProfile], UserProfileRepository
):
    @property
    def _table(self) -> Type[UserProfile]:
        return UserProfile

    @property
    def _entity(self) -> Type[UserProfileProps]:
        return UserProfileProps

    async def get_by_user_id(self, user_id: UUID) -> Optional[UserProfileProps]:
        async with self._db_session() as session:
            query = (
                self.select()
                .outerjoin(
                    Education,
                    and_(
                        Education.is_current == True,
                        Education.profile_id == UserProfile.id,
                        Education.deleted != True,
                    ),
                )
                .where(UserProfile.user_id == user_id)
                .options(contains_eager(self._table.educations))
            )
            result = await session.execute(query)
            obj = result.scalars().first()
            if not obj:
                return None
            return self._entity.from_orm(obj)

    async def get_profile_by_id(self, id: UUID) -> Optional[UserProfileProps]:
        async with self._db_session() as session:
            query = (
                self.select()
                .outerjoin(
                    Education,
                    and_(
                        Education.is_current == True,
                        Education.profile_id == UserProfile.id,
                        Education.deleted != True,
                    ),
                )
                .where(UserProfile.id == id)
                .options(contains_eager(UserProfile.educations))
            )
            result = await session.execute(query)
            obj = result.scalars().first()
            if not obj:
                return None
            return self._entity.from_orm(obj)

    async def create_profile(self, entity: UserProfileProps) -> None:
        async with self._db_session() as session:
            entity_dict = entity.dict(exclude={"educations"})
            query = self._table(**entity_dict)
            session.add(query)
            await session.commit()

    async def update_profile(self, entity: UserProfileProps) -> None:
        async with self._db_session() as session:
            entity_dict = entity.dict(exclude={"educations"})
            query = (
                update(self._table)  # type: ignore
                .where(self._table.id == entity.id)
                .values(**entity_dict)
                .execution_options(synchronize_session="fetch")
            )
            await session.execute(query)
            await session.commit()

    async def paginate_users(
        self, query: Any, page: int, page_size: int
    ) -> tuple[list[UserProfile], PageMetadata]:
        async with self._db_session() as session:
            if page <= 0:
                raise DomainError("page should be be >= 1")
            if page_size <= 0:
                raise DomainError("page_size should be >= 1")
            paginated_query = (
                query.order_by(desc(UserProfile.created_at))
                .limit(page_size)
                .offset((page - 1) * page_size)
            )
            count_query = query.with_only_columns(func.count(self._table.id))
            results = await session.execute(paginated_query)
            total_result = await session.execute(count_query)
            page_data = PageMetadata(
                page=page,
                page_size=page_size,
                total=total_result.scalar_one(),
            )
            return (results.scalars().unique().all(), page_data)

    async def _get_extra_profiles(self, query, nextra):
        """Get extra profiles from query

        query: query to run
        nextra: number of results to fetch
        """
        async with self._db_session() as session:
            count_query = query.with_only_columns(func.count(self._table.id))
            total_result = await session.execute(count_query)

            extra_query = query.order_by(
                desc(UserProfile.created_at)
            ).limit(nextra)
            results = await session.execute(extra_query)

            return results.scalars().unique().all(), total_result.scalar_one()

    async def get_suggested_users(
        self,
        user_id: UUID,
        include_profile_ids: list[UUID],
        exclude_profile_ids: list[UUID],
        page: int,
        page_size: int,
        text: str,
    ) -> Page[UserProfileProps]:
        query = (
            self.select()
            .where(UserProfile.user_id != user_id)
            .outerjoin(
                Education,
                and_(
                    Education.is_current == True,
                    Education.profile_id == UserProfile.id,
                    Education.deleted != True,
                ),
            )
            .options(contains_eager(UserProfile.educations))
        )
        if text:
            text = "".join(
                [c for c in text if c.isalnum() or c in [".", "-", " "]])
            text = "&".join(text.split())
            query = query.filter(UserProfile.__ts_vector__.match(f"{text}:*"))
        if include_profile_ids:
            query = query.where(self._table.id.in_(include_profile_ids))
        if exclude_profile_ids:
            query = query.where(self._table.id.notin_(exclude_profile_ids))
        results, page_metadata = await self.paginate_users(query, page, page_size)
        items = list(map(lambda obj: self._entity.from_orm(obj), results))
        return Page(items=items, **page_metadata.dict())

    async def search_profile_users(
        self,
        user_id: UUID,
        search_user_params: SearchUsersParams,
    ) -> Page[UserProfileProps]:
        query = (
            self.select()
            .where(UserProfile.user_id != user_id)
            .outerjoin(
                Education,
                and_(
                    Education.is_current == True,
                    Education.profile_id == UserProfile.id,
                    Education.deleted != True,
                ),
            )
            .options(contains_eager(UserProfile.educations))
        )

        return Page(items=items, **page_metadata.dict())

    async def get_by_parent_code(self, parent_code: str) -> Optional[UserProfileProps]:
        async with self._db_session() as session:
            query = (
                self.select()
                .outerjoin(
                    Education,
                    and_(
                        Education.is_current == True,
                        Education.profile_id == UserProfile.id,
                        Education.deleted != True,
                    ),
                )
                .where(UserProfile.parent_code == parent_code)
                .options(contains_eager(UserProfile.educations))
            )
            result = await session.execute(query)
            obj = result.scalars().first()
            if not obj:
                return None
            return self._entity.from_orm(obj)

    async def get_profiles(
        self, include_profile_ids: list[UUID]
    ) -> list[UserProfileProps]:
        async with self._db_session() as session:
            query = (
                self.select()
                .outerjoin(
                    Education,
                    and_(
                        Education.is_current == True,
                        Education.profile_id == UserProfile.id,
                        Education.deleted != True,
                    ),
                )
                .options(contains_eager(UserProfile.educations))
            ).where(self._table.id.in_(include_profile_ids))
            results = await session.execute(query)
            items = list(
                map(
                    lambda obj: self._entity.from_orm(obj),
                    results.scalars().unique().all(),
                )
            )
            return items

    async def _results_count(self, query):
        """Count of query results"""
        count_query = query.with_only_columns(func.count(self._table.id))
        async with self._db_session() as session:
            count = await session.execute(count_query)
            return count.scalar_one()

    def _extra_count(self, count, page):
        """Count of extra results needed for this page

        count: number of filtered results
        page: page meta data from filtered query
        """
        return max(page.page * page.page_size - page.total, 0)

    def _add_education_query(self, obj: Any, query_param):
        """Adds query on Education table

        obj: either self.select() or query, depending on if query was None
        query_param: list of Education institution IDs to filter on
        """
        query = obj.filter(
            UserProfile.profile_type == "STUDENT"
        ).join(
            Education,
            and_(
                Education.is_current == True,
                Education.profile_id == UserProfile.id,
                Education.deleted != True,
            ),
            full=True
        ).filter(
            Education.institution_id.in_(query_param),
        ).options(
            contains_eager(UserProfile.educations)
        )

        return query

    async def get_searched_users(
        self,
        page: int,
        page_size: int,
        search_user_params: SearchUsersParams
    ) -> Page[UserProfileProps]:
        query = None

        # FIXME: currently, the empty dicts as values in the original
        # mapping are for query parameters where search will be on karma tags.
        # We filter these out in the for-loop clause
        conditions = []
        for query_param_names, param in dict(
            (k, v) for k, v in PROFILE_QUERY_PARAMS_MAPPING.items() if v
        ).items():
            query_param = []
            for name in query_param_names:
                query_param += getattr(search_user_params, name)

            if not query_param:
                # No argument was given for this query parameter: skip it
                continue

            query_fields = param['query_fields']
            if param['table'] == UserProfile:
                # Query on this table
                for name in query_fields:
                    field = getattr(UserProfile, name)
                    conditions.append(field.in_(query_param))
            else:
                # Query on another table: do a JOIN
                match_id = param.get('match_id')
                if match_id is None:
                    table_id = getattr(param['table'], 'profile_id')
                    userprofile_id = UserProfile.id
                else:
                    table_id = getattr(param['table'], match_id['table_id'])
                    userprofile_id = UserProfile.user_id

                deleted = getattr(param['table'], 'deleted')

                for name in query_fields:
                    field = getattr(param['table'], name)
                    conditions.append(field.in_(query_param))

                if query is None:
                    query = self.select().join(
                        param['table'],
                        and_(
                            table_id == userprofile_id,
                            deleted != True
                        ),
                        full=True
                    )
                else:
                    query = query.outerjoin(
                        param['table'],
                        and_(
                            table_id == userprofile_id,
                            deleted != True
                        ),
                        full=True
                    )

        if conditions:
            if query is None:
                # Might happen if only UserProfile table is queried
                query = self.select().filter(or_(*conditions))
            else:
                query = query.filter(or_(*conditions))

        query_param = getattr(search_user_params, 'SCHOOL')
        if query is None:
            if query_param:
                query = self._add_education_query(self.select(), query_param)
            else:
                query = self.select().filter(
                    UserProfile.profile_type == "STUDENT"
                ).outerjoin(Education).options(
                    contains_eager(UserProfile.educations)
                )
        else:
            # Additional filter on existing query
            if query_param:
                query = self._add_education_query(query, query_param)
            else:
                query = query.filter(
                    UserProfile.profile_type == "STUDENT"
                ).outerjoin(Education).options(
                    contains_eager(UserProfile.educations)
                )

        query = query.distinct()
        results, page_metadata = await self.paginate_users(
            query, page, page_size
        )

        # temporarily disabling all records return
        metadata = page_metadata.dict()

        # if results:
        #     # Check if additional results are needed
        #     count = await self._results_count(query)
        #     nextra = self._extra_count(count, page_metadata)

        #     if nextra:
        #         fetched_ids = [obj.id for obj in results]
        #         query = self.select().filter(
        #             UserProfile.profile_type == "STUDENT",
        #             self._table.id.notin_(fetched_ids),
        #         ).outerjoin(Education).options(
        #             contains_eager(UserProfile.educations)
        #         )
        #         extra_results, total = await self._get_extra_profiles(
        #             query, nextra
        #         )

        #         metadata = page_metadata.dict()
        #         metadata['total'] += total
        #         results += extra_results
        #     else:
        #         metadata = page_metadata.dict()
        # else:
        #     # Return all student profiles if query does not return
        #     query = self.select().filter(
        #         UserProfile.profile_type == "STUDENT"
        #     ).outerjoin(Education).options(
        #         contains_eager(UserProfile.educations)
        #     )
        #     results, page_metadata = await self.paginate_users(
        #         query, page, page_size
        #     )
        #     metadata = page_metadata.dict()

        items = list(map(lambda obj: self._entity.from_orm(obj), results))
        return Page(items=items, **metadata)
