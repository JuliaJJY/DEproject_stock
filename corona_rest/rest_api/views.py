from drf_yasg.utils import swagger_auto_schema
from drf_yasg.openapi import Parameter, IN_QUERY, TYPE_STRING
from drf_yasg.utils import swagger_auto_schema
from django.contrib.auth.models import User, Group
from rest_framework import viewsets, permissions
from rest_api.models import *
from rest_api.serializers import * 
from django.http import *
from django.http import HttpResponse
from datetime import datetime, timedelta, date


def get_queryset_by_date(model, query_params):
    if ~('start_date' in query_params) and ~('end_date' in query_params):
        queryset = model.objects.filter(std_day__gt=(datetime.today() - timedelta(7)))

    if 'start_date' in query_params and 'end_date' in query_params:

        start_date = query_params['start_date']
        end_date = query_params['end_date']
        queryset = model.objects.filter(std_day__range=(date.fromisoformat(start_date)
                                                        , date.fromisoformat(end_date)))

    if 'start_date' in query_params:
        start_date = query_params['start_date']
        queryset = model.objects.filter(std_day__gt=start_date)

    if 'end_date' in query_params:
        end_date = query_params['end_date']
        queryset = model.objects.filter(std_day__lt=end_date)

    return queryset


class CoFacilityViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = CoFacility.objects.all().order_by('-std_day')
    serializer_class = CoFacilitySerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="10만명당 다중이용시설의 개수와 10만명 당 코로나 발생자 수",
        operation_description="""시작날짜와 끝날짜를 모두 생략하면 최근 1주일 데이터를 반환합니다. <br>
            시작날짜만 입력하면 시작날짜 이후의 데이터를 반환합니다.<br>
            끝날짜만 입력하면 끝날짜 이전 데이터를 반환합니다.<br> """,
        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd)", required=True),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd)", required=True),
        ],
    )

    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(CoFacility, query_params)

        print('params : >>>>>>>>>', query_params)
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)


    




class CoPopuDensityUserViewSet(viewsets.ReadOnlyModelViewSet):
    
    queryset = CoPopuDensity.objects.all().order_by('-std_day')
    serializer_class = CoPopuDensitySerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="인구밀도와 10만명당 코로나 발생 환자 간의 상관관계",
        operation_description="""시작날짜와 끝날짜를 모두 생략하면 최근 1주일 데이터를 반환합니다. <br>
        시작날짜만 입력하면 시작날짜 이후의 데이터를 반환합니다.<br>
        끝날짜만 입력하면 끝날짜 이전 데이터를 반환합니다.<br>
         """,
        manual_parameters=[
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
        ],
    )

    def list(self, request):
        query_params = request.query_params
        queryset = get_queryset_by_date(CoPopuDensity, query_params)
        print('params : >>>>>>>>>', query_params)
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data, safe=False)


class CoVaccineViewSet(viewsets.ReadOnlyModelViewSet):
    
    queryset = CoVaccine.objects.all().order_by('-std_day')
    serializer_class = CoVaccineSerializer
    permission_classes = [permissions.IsAuthenticated]

class CoWeekdayViewSet(viewsets.ReadOnlyModelViewSet):
   
    queryset = CoWeekday.objects.all().order_by('-std_day')
    serializer_class = CoWeekdaySerializer
    permission_classes = [permissions.IsAuthenticated]
